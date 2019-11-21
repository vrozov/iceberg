/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg

import com.google.common.collect.ImmutableList
import java.util.{Map => JMap, UUID}
import org.apache.hadoop.fs.Path
import org.apache.iceberg.io.{FileIO, OutputFile}
import org.apache.iceberg.spark.SparkTableUtil.Manifest
import org.apache.iceberg.util.{BinPacking, Tasks}
import org.apache.iceberg.util.JavaFunctionImplicits._
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, SQLContext, SQLImplicits}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * An action that rewrites manifests according to a given strategy.
 */
private[iceberg] case class RewriteManifestsAction[K](
    strategy: RewriteManifestStrategy[K])(implicit i: K => Ordered[K]) extends IcebergAction {

  private val log = LoggerFactory.getLogger(getClass)

  def execute(spark: SparkSession, table: Table): Unit = {
    val matchingManifests = findMatchingManifests(table)
    if (matchingManifests.isEmpty) {
      return
    }

    val io = table.io
    val specs = table.specs
    val avgManifestEntrySizeInBytes = ActionUtil.computeAvgManifestEntrySizeInBytes(
      matchingManifests,
      strategy.defaultManifestEntrySizeInBytes)

    val fileDS = buildFileDS(spark, matchingManifests, specs, io)
    fileDS.cache()

    try {
      val keyMetadataSizes = computeMetadataSizePerKey(spark, fileDS, avgManifestEntrySizeInBytes)
      val bins = computeBins(keyMetadataSizes, strategy.targetManifestSizeInBytes)
      val newManifests = writeManifests(spark, fileDS, bins, specs, io)
      replaceManifests(table, matchingManifests, newManifests)
    } finally {
      fileDS.unpersist(blocking = false)
    }
  }

  private def findMatchingManifests(table: Table): Seq[ManifestFile] = {
    if (table.currentSnapshot == null) {
      return Seq.empty
    }
    table.currentSnapshot.manifests.asScala.filter(strategy.shouldRewrite)
  }

  // builds a dataset of files with their partition spec ids, snapshot ids, clustering keys
  private def buildFileDS(
      spark: SparkSession,
      manifests: Seq[ManifestFile],
      specs: JMap[Integer, PartitionSpec],
      io: FileIO): Dataset[(Int, Long, K, DataFile)] = {

    val implicits = new Implicits(spark, strategy.clusterKeyEncoder)
    import implicits._

    val manifestPaths = manifests.map(_.path)
    val numPartitions = spark.conf.get("spark.default.parallelism", manifestPaths.size.toString)
    val manifestPathDS = spark.sparkContext.parallelize(manifestPaths, numPartitions.toInt).toDS()
    manifestPathDS.flatMap { path =>
      val reader = ManifestReader.read(io.newInputFile(path), specs)
      val spec = reader.spec
      try {
        val filteredManifest = reader.select(ImmutableList.of("*"))
        filteredManifest.liveEntries.asScala.map { entry =>
          val file = entry.file.copy()
          val snapshotId = entry.snapshotId
          (spec.specId, snapshotId, strategy.clusterBy(spec, file), file)
        }
      } finally {
        reader.close()
      }
    }
  }

  // computes the total size of metadata per clustering key
  private def computeMetadataSizePerKey(
      spark: SparkSession,
      fileDS: Dataset[(Int, Long, K, DataFile)],
      manifestEntrySizeInBytes: Long): Seq[(Int, K, Long)] = {

    val implicits = new Implicits(spark, strategy.clusterKeyEncoder)
    import implicits._

    val keyMetadataSizes = fileDS
      .map { case (specId, _, key, _) => (specId, key, manifestEntrySizeInBytes) }
      .groupByKey { case (specId, key, _) => (specId, key) }
      .mapValues { case (_, _, metadataSize) => metadataSize }
      .reduceGroups(_ + _)
      .map { case ((specId, key), totalMetadataSize) => (specId, key, totalMetadataSize) }
      .collect()

    keyMetadataSizes.toSeq
  }

  // computes bins for the given strategy
  private def computeBins(
      keyMetadataSizes: Seq[(Int, K, Long)],
      targetManifestSizeInBytes: Long): Map[(Int, K), String] = {

    val binMap = mutable.HashMap.empty[(Int, K), String]

    // keep files with different partition specs separate and sort entries by clustering key
    val specMetadataSizes = keyMetadataSizes
      .groupBy { case (specId, _, _) => specId }
      .mapValues { sizeSummaries =>
        sizeSummaries
          .map { case (_, key, size) => (key, size) }
          .sortBy { case (key, _) => key }
      }

    // bin-pack groups to achieve the desired manifest size
    specMetadataSizes.foreach { case (specId, keySizeSummaries) =>
      val packer = new BinPacking.ListPacker[(K, Long)](targetManifestSizeInBytes, 1, false)
      val bins = packer.pack(keySizeSummaries.asJava, toJavaFunction { case (_, size) => size })
      bins.asScala.zipWithIndex.foreach { case (bin, index) =>
        bin.asScala.foreach { case (key, _) =>
          binMap += (specId, key) -> s"specId=$specId,binId=$index"
        }
      }
    }

    binMap.toMap
  }

  // groups files according to the computed bins and writes them into new manifests
  private def writeManifests(
      spark: SparkSession,
      fileDS: Dataset[(Int, Long, K, DataFile)],
      bins: Map[(Int, K), String],
      specs: JMap[Integer, PartitionSpec],
      io: FileIO): Array[ManifestFile] = {

    val implicits = new Implicits(spark, strategy.clusterKeyEncoder)
    import implicits._

    // the size of bins is estimated based on the available stats
    // we allow the actual size of manifests to be 2% bigger in order
    // to decrease the chance of closing manifests near the end of bins and
    // to prevent having manifests with a couple of entries
    val targetManifestSizeInBytes = 1.02 * strategy.targetManifestSizeInBytes
    val stagingLocation = strategy.stagingLocation

    val manifests = fileDS
      .groupByKey { case (specId, _, key, _) => (specId, bins(specId, key)) }
      .mapValues { case (_, snapshotId, _, file) => (snapshotId, file) }
      .flatMapGroups { case ((specId, _), fileIter) =>
        val ctx = TaskContext.get()

        def manifestFileSupplier: OutputFile = {
          val id = UUID.randomUUID()
          val fileName = s"stage-${ctx.stageId}-task-${ctx.taskAttemptId}-id-$id-manifest"
          val location = new Path(stagingLocation, fileName)
          io.newOutputFile(FileFormat.AVRO.addExtension(location.toString))
        }

        val spec = specs.get(specId)
        val writer = new ManifestsWriter(spec, manifestFileSupplier, targetManifestSizeInBytes.toLong)
        try {
          fileIter.foreach { case (snapshotId, file) => writer.existing(file, snapshotId) }
        } finally {
          writer.close()
        }

        writer.manifests.asScala.map(m => Manifest(m.path, m.length, m.partitionSpecId))
      }
      .collect()

    manifests.map(_.toManifestFile)
  }

  private def replaceManifests(
      table: Table,
      deletedManifests: Iterable[ManifestFile],
      newManifests: Iterable[ManifestFile]): Unit = {

    try {
      val rewriteManifests = table.rewriteManifests()
      deletedManifests.foreach(rewriteManifests.deleteManifest)
      newManifests.foreach(rewriteManifests.addManifest)
      rewriteManifests.commit()
    } finally {
      // clean up all staged manifests
      Tasks.foreach(newManifests.asJava)
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure { (manifest: ManifestFile, e: Exception) =>
          log.warn(s"Could not delete a staged manifest: ${manifest.path}", e)
        }
        .run((manifest: ManifestFile) => table.io.deleteFile(manifest.path))
    }
  }

  // extends the built-in implicits with extra encoders for performed transformations
  private class Implicits(spark: SparkSession, keyEncoder: Encoder[K]) extends SQLImplicits {
    override protected def _sqlContext: SQLContext = spark.sqlContext // scalastyle:ignore

    implicit val e1: Encoder[DataFile] = Encoders.javaSerialization[DataFile]
    implicit val e2: Encoder[(Int, K)] = Encoders.tuple(
      Encoders.scalaInt, strategy.clusterKeyEncoder)
    implicit val e3: Encoder[(Long, DataFile)] = Encoders.tuple(
      Encoders.scalaLong, Encoders.javaSerialization[DataFile])
    implicit val e4: Encoder[(Int, K, Long)] = Encoders.tuple(
      Encoders.scalaInt, strategy.clusterKeyEncoder, Encoders.scalaLong)
    implicit val e5: Encoder[(Int, Long, String, DataFile)] = Encoders.tuple(
      Encoders.scalaInt, Encoders.scalaLong, Encoders.STRING, Encoders.javaSerialization[DataFile])
    implicit val e6: Encoder[(Int, Long, K, DataFile)] = Encoders.tuple(
      Encoders.scalaInt, Encoders.scalaLong, strategy.clusterKeyEncoder, Encoders.javaSerialization[DataFile])
  }
}
