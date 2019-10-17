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

package org.apache.iceberg;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.OutputFile;

/**
 * A manifest writer that splits entries between multiple files in order to produce files of the configured size.
 */
public class ManifestsWriter implements Closeable {
  private final PartitionSpec spec;
  private final long snapshotId;
  private final Supplier<OutputFile> outputFileSupplier;
  private final long targetManifestSizeBytes;
  private final List<ManifestFile> manifests;
  private ManifestWriter writer;
  private long currentNumEntries;

  public ManifestsWriter(PartitionSpec spec, long snapshotId,
                         Supplier<OutputFile> outputFileSupplier,
                         long targetManifestSizeBytes) {
    this.spec = spec;
    this.snapshotId = snapshotId;
    this.outputFileSupplier = outputFileSupplier;
    this.targetManifestSizeBytes = targetManifestSizeBytes;
    this.manifests = Lists.newArrayList();
    this.currentNumEntries = 0;
  }

  public ManifestsWriter(PartitionSpec spec,
                         Supplier<OutputFile> outputFileSupplier,
                         long targetManifestSizeBytes) {
    this(spec, -1, outputFileSupplier, targetManifestSizeBytes);
  }

  public void add(DataFile addedFile) {
    lazyWriter().add(addedFile);
    currentNumEntries++;
  }

  void add(ManifestEntry entry) {
    lazyWriter().add(entry);
    currentNumEntries++;
  }

  public void existing(DataFile existingFile, long fileSnapshotId) {
    lazyWriter().existing(existingFile, fileSnapshotId);
    currentNumEntries++;
  }

  void existing(ManifestEntry entry) {
    lazyWriter().existing(entry);
    currentNumEntries++;
  }

  public void delete(DataFile deletedFile) {
    lazyWriter().delete(deletedFile);
    currentNumEntries++;
  }

  void delete(ManifestEntry entry) {
    lazyWriter().delete(entry);
    currentNumEntries++;
  }

  public void close() {
    if (writer != null) {
      try {
        writer.close();
        manifests.add(writer.toManifestFile());
        currentNumEntries = 0;
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
  }

  public List<ManifestFile> manifests() {
    return manifests;
  }

  private ManifestWriter lazyWriter() {
    // verify the size of the current manifest every 25 entries to avoid calling writer.length() every time
    if (writer == null) {
      writer = new ManifestWriter(spec, outputFileSupplier.get(), snapshotId);
    } else if (currentNumEntries % 25 == 0 && writer.length() >= targetManifestSizeBytes) {
      close();
      writer = new ManifestWriter(spec, outputFileSupplier.get(), snapshotId);
    }
    return writer;
  }
}
