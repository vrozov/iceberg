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
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Projections;

public class DefaultModifyFiles extends MergingSnapshotProducer<ModifyFiles> implements ModifyFiles {

  private boolean validate = false;
  private Long baseSnapshotId = null;
  private Expression fileFilter = null;

  DefaultModifyFiles(TableOperations ops) {
    super(ops);

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
    Preconditions.checkArgument(filesToAdd != null, "Files to add can not be null");

    for (DataFile toDelete : filesToDelete) {
      delete(toDelete.path());
    }

    for (DataFile toAdd : filesToAdd) {
      add(toAdd);
    }

    return this;
  }

  @Override
  public ModifyFiles validate(Long snapshotId) {
    return validate(snapshotId, null);
  }

  @Override
  public ModifyFiles validate(Long snapshotId, Expression expr) {
    this.baseSnapshotId = snapshotId;
    this.fileFilter = expr;
    this.validate = true;
    return this;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    if (validate) {
      Long currentId = base.currentSnapshot() == null ? null : base.currentSnapshot().snapshotId();

      if (fileFilter == null) {
        ValidationException.check(Objects.equals(baseSnapshotId, currentId),
            "Modify operation requires no changes to timeline. Base snapshot %s != %s",
            baseSnapshotId, currentId);
      } else {
        PartitionSpec spec = writeSpec();
        Expression inclusiveExpr = Projections.inclusive(spec).project(fileFilter);
        Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);

        List<DataFile> collectedFiles = collectAddedFilesSince(base, currentId);
        for (DataFile file : collectedFiles) {
          ValidationException.check(!inclusive.eval(file.partition()),
              "Modify operation requires no in-range changes to timeline. File filter %s applies to %s",
              fileFilter, file.path());
        }
      }
    }

    return super.apply(base);
  }

  private List<DataFile> collectAddedFilesSince(TableMetadata base,
                                                Long currentSnapshotId) {
    List<DataFile> collectedFiles = new ArrayList<>();

    Long currentId = currentSnapshotId;
    while (currentId != null && !Objects.equals(currentId, baseSnapshotId)) {
      Snapshot currentSnapshot = base.snapshot(currentId);

      if (currentSnapshot == null) {
        throw new ValidationException(
            "Modify operation requires to timeline to be present. Snapshot %d has been expired", currentId);
      }

      Iterators.addAll(collectedFiles, currentSnapshot.addedFiles().iterator());
      currentId = currentSnapshot.parentId();
    }

    return collectedFiles;
  }
}
