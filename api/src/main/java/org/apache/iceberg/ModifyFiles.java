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

import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;

/**
 * API for modifying files in a table.
 * <p>
 * This API accumulates file additions and deletions, produces a new {@link Snapshot} of the
 * changes, and commits that snapshot as the current.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 * <p>
 * Similarly to {@link RewriteFiles}, if any of the deleted files are no longer in the latest snapshot
 * when reattempting, the commit will throw a {@link ValidationException}.
 */
public interface ModifyFiles extends SnapshotUpdate<ModifyFiles> {
  /**
   * Add a modify that replaces one set of files with another set that might contain different data.
   *
   * @param filesToDelete files that will be replaced (deleted), cannot be null.
   * @param filesToAdd    files that will be added, cannot be null.
   * @return this for method chaining
   */
  ModifyFiles modifyFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd);

  /**
   * Signals validation of the timeline for operations that happened between the base snapshot,
   * from which data has been read, to the current snapshot, to which data is being written.
   * <p>
   * If this method is called the snapshot provide must be the parent of the current snapshot
   * at commit time, effectively disabling parallel writes.
   *
   * @param snapshotId of the base snapshot
   * @return this for method chaining
   */
  ModifyFiles validate(Long snapshotId);

  /**
   * Signals validation of the timeline for operations that happened between the base snapshot,
   * from which data has been read, to the current snapshot, to which data is being written.
   * <p>
   * If this method is called, each added file on all snapshots between the base snapshot and the
   * current snapshot are validated to ensure the operation creating the current snapshot wouldn't
   * affect those new files as well.
   *
   * @param snapshotId of the base snapshot
   * @param expr       an expression on rows in the table
   * @return this for method chaining
   */
  ModifyFiles validate(Long snapshotId, Expression expr);
}
