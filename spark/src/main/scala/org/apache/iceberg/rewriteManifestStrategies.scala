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

import org.apache.spark.sql.{Encoder, Encoders}
import scala.collection.JavaConverters._

trait RewriteManifestStrategy[K] extends Serializable {
  def targetManifestSizeInBytes: Long
  def defaultManifestEntrySizeInBytes: Long
  def stagingLocation: String
  def shouldRewrite(manifest: ManifestFile): Boolean
  def clusterKeyEncoder: Encoder[K]
  def clusterBy(spec: PartitionSpec, file: DataFile)(implicit i: K => Ordered[K]): K
}

case class PartitionBasedRewriteManifestsStrategy(
    targetManifestSizeInBytes: Long,
    manifestSizeThreshold: Double,
    defaultManifestEntrySizeInBytes: Long,
    stagingLocation: String) extends RewriteManifestStrategy[String] {

  require(targetManifestSizeInBytes > 0)
  require(manifestSizeThreshold >= 0 && manifestSizeThreshold <= 1)
  require(defaultManifestEntrySizeInBytes > 0)

  override def shouldRewrite(manifest: ManifestFile): Boolean = {
    // rewrite manifests only if their size is not optimal or
    // if they contain files for more than one partition
    !hasOptimalSize(manifest) || !coversOnePartition(manifest)
  }

  private def hasOptimalSize(manifest: ManifestFile): Boolean = {
    val manifestSizeDiff = Math.abs(targetManifestSizeInBytes - manifest.length)
    manifestSizeDiff < manifestSizeThreshold * targetManifestSizeInBytes
  }

  private def coversOnePartition(manifest: ManifestFile): Boolean = {
    manifest.partitions == null || manifest.partitions.asScala.forall(p => p.lowerBound == p.upperBound)
  }

  override def clusterKeyEncoder: Encoder[String] = Encoders.STRING

  override def clusterBy(spec: PartitionSpec, file: DataFile)(implicit i: String => Ordered[String]): String = {
    spec.partitionToPath(file.partition)
  }
}
