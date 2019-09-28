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

import com.google.common.collect.{Lists, Maps}
import java.io.File
import java.nio.file.{Files => JFiles}
import java.util.{List => JList}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.TableProperties.{MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.source.SimpleRecord
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField.optional
import org.apache.iceberg.util.PropertyUtil
import org.apache.spark.sql.{Encoders, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.scalatestplus.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class RewriteManifestsActionSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private val schema = new Schema(
    optional(1, "id", Types.IntegerType.get),
    optional(2, "data", Types.StringType.get))
  private val conf = new Configuration()

  private var spark: SparkSession = _
  private var tableLocation: File = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()
  }

  override def beforeEach(): Unit = {
    val tempDir = JFiles.createTempDirectory("iceberg-table-")
    tableLocation = tempDir.toFile
  }

  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(tableLocation)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("rewrite manifests in an empty table") {
    val tables = new HadoopTables(conf)

    val spec = PartitionSpec.unpartitioned
    val options = Maps.newHashMap[String, String]
    val table = tables.create(schema, spec, options, tableLocation.toString)

    require(table.currentSnapshot == null, "table should be empty")

    val actions = IcebergActions.forTable(table)

    val targetManifestSizeInBytes = PropertyUtil.propertyAsLong(
      table.properties, MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT)
    val strategy = PartitionBasedRewriteManifestsStrategy(
      targetManifestSizeInBytes,
      manifestSizeThreshold = 0.25,
      defaultManifestEntrySizeInBytes = 1L,
      stagingLocation = System.getProperty("java.io.tmpdir"))
    actions.rewriteManifests(strategy)

    table.refresh()
    assert(table.currentSnapshot == null, "no snapshot should be produced")
  }

  test("rewrite manifests in a non-partitioned table") {
    val tables = new HadoopTables(conf)

    val spec = PartitionSpec.unpartitioned
    val options = Maps.newHashMap[String, String]
    val table = tables.create(schema, spec, options, tableLocation.toString)

    require(table.currentSnapshot == null, "table should be empty")

    val records1 = Lists.newArrayList(new SimpleRecord(1, "1"))
    writeRecords(records1)

    val records2 = Lists.newArrayList(new SimpleRecord(2, "2"))
    writeRecords(records2)

    table.refresh()

    val manifests = table.currentSnapshot.manifests
    assert(manifests.size == 2, "should have 2 manifests")

    val actions = IcebergActions.forTable(table)

    val strategy = PartitionBasedRewriteManifestsStrategy(
      targetManifestSizeInBytes = Long.MaxValue,
      manifestSizeThreshold = 0.25,
      defaultManifestEntrySizeInBytes = 1L,
      stagingLocation = System.getProperty("java.io.tmpdir"))
    actions.rewriteManifests(strategy)

    table.refresh()

    val newManifests = table.currentSnapshot.manifests
    assert(newManifests.size == 1, "should create one manifest")

    val newManifest = newManifests.get(0)
    assert(newManifest.existingFilesCount == 2)
    assert(!newManifest.hasAddedFiles)
    assert(!newManifest.hasDeletedFiles)

    val expectedRecords = Lists.newArrayList[SimpleRecord]()
    expectedRecords.addAll(records1)
    expectedRecords.addAll(records2)

    val result = spark.read.format("iceberg").load(tableLocation.toString)
    val actualRecords = result
      .sort("id")
      .as[SimpleRecord](Encoders.bean(classOf[SimpleRecord]))
      .collectAsList()

    assert(expectedRecords == actualRecords)
  }

  test("write multiple manifests for one key") {
    val tables = new HadoopTables(conf)

    val spec = PartitionSpec.unpartitioned
    val options = Maps.newHashMap[String, String]
    val table = tables.create(schema, spec, options, tableLocation.toString)

    require(table.currentSnapshot == null, "table should be empty")

    // ManifestsWriter checks the current size every 25 entries,
    // so we need to have 25+ manifest entries to produce multiple manifests
    val records = Lists.newArrayList[SimpleRecord]()
    for (i <- 1 to 50) {
      records.add(new SimpleRecord(i, i.toString))
    }
    writeRecords(records)

    table.refresh()

    val manifests = table.currentSnapshot.manifests
    assert(manifests.size == 1, "should have 1 manifests")

    val actions = IcebergActions.forTable(table)

    val strategy = PartitionBasedRewriteManifestsStrategy(
      targetManifestSizeInBytes = 1L,
      manifestSizeThreshold = 0,
      defaultManifestEntrySizeInBytes = 1L,
      stagingLocation = System.getProperty("java.io.tmpdir"))
    actions.rewriteManifests(strategy)

    table.refresh()

    val newManifests = table.currentSnapshot.manifests
    assert(newManifests.size == 2)

    val result = spark.read.format("iceberg").load(tableLocation.toString)
    val actualRecords = result
      .sort("id")
      .as[SimpleRecord](Encoders.bean(classOf[SimpleRecord]))
      .collectAsList()

    assert(records == actualRecords)
  }

  test("combine multiple manifests for different keys") {
    val tables = new HadoopTables(conf)

    val spec = PartitionSpec.builderFor(schema)
      .identity("data")
      .build()
    val options = Maps.newHashMap[String, String]
    val table = tables.create(schema, spec, options, tableLocation.toString)

    require(table.currentSnapshot == null, "table should be empty")

    val records1 = Lists.newArrayList(
      new SimpleRecord(1, "1"),
      new SimpleRecord(2, "2"))
    writeRecords(records1)

    val records2 = Lists.newArrayList(
      new SimpleRecord(3, "3"), // scalastyle:ignore
      new SimpleRecord(4, "4")) // scalastyle:ignore
    writeRecords(records2)

    val records3 = Lists.newArrayList(
      new SimpleRecord(5, "5"), // scalastyle:ignore
      new SimpleRecord(6, "6")) // scalastyle:ignore
    writeRecords(records3)

    val records4 = Lists.newArrayList(
      new SimpleRecord(7, "7"), // scalastyle:ignore
      new SimpleRecord(8, "8")) // scalastyle:ignore
    writeRecords(records4)

    table.refresh()

    val manifests = table.currentSnapshot.manifests
    assert(manifests.size == 4, "should have 4 manifests")

    val actions = IcebergActions.forTable(table)

    val avgManifestEntrySizeInBytes = ActionUtil.computeAvgManifestEntrySizeInBytes(manifests.asScala, 1L)
    val targetManifestSizeInBytes = 4 * avgManifestEntrySizeInBytes
    val strategy = PartitionBasedRewriteManifestsStrategy(
      targetManifestSizeInBytes,
      manifestSizeThreshold = 0,
      defaultManifestEntrySizeInBytes = 1L,
      stagingLocation = System.getProperty("java.io.tmpdir"))
    actions.rewriteManifests(strategy)

    table.refresh()

    val newManifests = table.currentSnapshot.manifests
    assert(newManifests.size == 2)

    assert(newManifests.get(0).existingFilesCount == 4)
    assert(!newManifests.get(0).hasAddedFiles)
    assert(!newManifests.get(0).hasDeletedFiles)

    assert(newManifests.get(1).existingFilesCount == 4)
    assert(!newManifests.get(1).hasAddedFiles)
    assert(!newManifests.get(1).hasDeletedFiles)

    val expectedRecords = Lists.newArrayList[SimpleRecord]()
    expectedRecords.addAll(records1)
    expectedRecords.addAll(records2)
    expectedRecords.addAll(records3)
    expectedRecords.addAll(records4)

    val result = spark.read.format("iceberg").load(tableLocation.toString)
    val actualRecords = result
      .sort("id")
      .as[SimpleRecord](Encoders.bean(classOf[SimpleRecord]))
      .collectAsList()

    assert(expectedRecords == actualRecords)
  }

  test("should not rewrite optimal manifests") {
    val tables = new HadoopTables(conf)

    val spec = PartitionSpec.unpartitioned
    val options = Maps.newHashMap[String, String]
    val table = tables.create(schema, spec, options, tableLocation.toString)

    require(table.currentSnapshot == null, "table should be empty")

    val records = Lists.newArrayList(new SimpleRecord(1, "1"))
    writeRecords(records)

    table.refresh()

    val firstSnapshot = table.currentSnapshot
    val firstSnapshotManifests = firstSnapshot.manifests
    assert(firstSnapshotManifests.size == 1, "should have 1 manifests")

    val actions = IcebergActions.forTable(table)

    val targetManifestSizeInBytes = firstSnapshotManifests.get(0).length
    val strategy = PartitionBasedRewriteManifestsStrategy(
      targetManifestSizeInBytes,
      manifestSizeThreshold = 0.05,
      defaultManifestEntrySizeInBytes = 1L,
      stagingLocation = System.getProperty("java.io.tmpdir"))
    actions.rewriteManifests(strategy)

    table.refresh()

    assert(table.currentSnapshot.snapshotId == firstSnapshot.snapshotId, "should not create a new manifest")
  }

  private def writeRecords(records: JList[SimpleRecord]): Unit = {
    val df = spark.createDataFrame(records, classOf[SimpleRecord])
    df.repartition(records.size, df("id"))
      .select("id", "data")
      .write
      .format("iceberg")
      .mode("append")
      .save(tableLocation.toString)
  }
}
