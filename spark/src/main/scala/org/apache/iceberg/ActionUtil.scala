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

import scala.collection.JavaConverters._

private[iceberg] object ActionUtil {

  def buildSpecLookup(table: Table): Integer => PartitionSpec = {
    val specMap = table.specs.asScala
    id => specMap(id)
  }

  def computeAvgManifestEntrySizeInBytes(manifests: Iterable[ManifestFile], defaultSize: Long): Long = {
    var totalSize = 0L
    var numEntries = 0

    val manifestsWithFileCounts = manifests.filter(containsFilesCounts)
    manifestsWithFileCounts.foreach { manifest =>
      totalSize += manifest.length
      numEntries += manifest.addedFilesCount + manifest.existingFilesCount + manifest.deletedFilesCount
    }

    if (totalSize != 0 && numEntries != 0) totalSize / numEntries else defaultSize
  }

  private def containsFilesCounts(manifest: ManifestFile): Boolean = {
    manifest.addedFilesCount != null && manifest.existingFilesCount != null && manifest.deletedFilesCount != null
  }
}
