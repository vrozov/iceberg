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
   * @param filesToDelete files that will be replaced (deleted), cannot be null
   * @param filesToAdd    files that will be added, cannot be null
   * @return this for method chaining
   */
  ModifyFiles modifyFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd);

  /**
   * Enables validation of new files that are added concurrently after the modify operation starts.
   *
   * If another concurrent operation commits a new file that might contain rows matching
   * rowFilter expression, the modify operation will detect this during retries and fail.
   *
   * @param rowFilter an expression on rows in the table
   * @return this for method chaining
   */
  ModifyFiles failOnNewFiles(Expression rowFilter);
}
