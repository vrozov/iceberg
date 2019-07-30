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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Projections;

public class DefaultModifyFiles extends MergingSnapshotProducer<ModifyFiles> implements ModifyFiles {

  private Long baseSnapshotId;
  private Expression rowFilter;

  DefaultModifyFiles(TableOperations ops, Long baseSnapshotId) {
    super(ops);
    this.baseSnapshotId = baseSnapshotId;
    // modify files must fail if any of the deleted paths is missing and cannot be deleted
    failMissingDeletePaths();
  }

  @Override
  protected ModifyFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.MODIFY;
  }

  @Override
  public ModifyFiles modifyFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd) {
    Preconditions.checkArgument(filesToDelete != null, "Files to delete cannot be null");
    Preconditions.checkArgument(filesToAdd != null, "Files to add cannot be null");

    for (DataFile toDelete : filesToDelete) {
      delete(toDelete.path());
    }

    for (DataFile toAdd : filesToAdd) {
      add(toAdd);
    }

    return this;
  }

  @Override
  public ModifyFiles failOnNewFiles(Expression newRowFilter) {
    Preconditions.checkArgument(newRowFilter != null, "Row filter cannot be null");
    this.rowFilter = newRowFilter;
    return this;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    if (rowFilter != null) {
      PartitionSpec spec = writeSpec();
      Expression inclusiveExpr = Projections.inclusive(spec).project(rowFilter);
      Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);

      List<DataFile> newFiles = collectNewFiles(base);
      for (DataFile newFile : newFiles) {
        // we do partition-level conflict resolution right now
        // we can enhance it by leveraging column stats and MetricsEvaluator
        ValidationException.check(!inclusive.eval(newFile.partition()),
            "Modify operation detected a new file %s that might match %s", newFile.path(), rowFilter);
      }
    }

    return super.apply(base);
  }

  private List<DataFile> collectNewFiles(TableMetadata meta) {
    Long currentSnapshotId = meta.currentSnapshot() == null ? null : meta.currentSnapshot().snapshotId();

    List<DataFile> newFiles = new ArrayList<>();

    Long currentId = currentSnapshotId;
    while (currentId != null && !Objects.equals(currentId, baseSnapshotId)) {
      Snapshot currentSnapshot = meta.snapshot(currentId);

      if (currentSnapshot == null) {
        throw new ValidationException(
            "Modify operation cannot find snapshot %d. Was it expired?", currentId);
      }

      Iterables.addAll(newFiles, currentSnapshot.addedFiles());
      currentId = currentSnapshot.parentId();
    }

    return newFiles;
  }
}
