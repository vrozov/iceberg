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

package org.apache.iceberg.spark.source;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ModifyFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitFunctions {

  private static final Logger LOG = LoggerFactory.getLogger(CommitFunctions.class);

  public interface CommitFunction {
    void apply(Table table, Iterable<DataFile> newFiles);
  }

  public static class Append implements CommitFunction {
    private static final Append INSTANCE = new Append();

    public static Append get() {
      return INSTANCE;
    }

    @Override
    public void apply(Table table, Iterable<DataFile> newFiles) {
      AppendFiles append = table.newAppend();

      int numFiles = 0;
      for (DataFile file : newFiles) {
        numFiles += 1;
        append.appendFile(file);
      }

      LOG.info("Committing append with {} files to table {}", numFiles, table);
      long start = System.currentTimeMillis();
      append.commit();
      long duration = System.currentTimeMillis() - start;
      LOG.info("Committed in {} ms", duration);
    }
  }

  public static class DynamicPartitionOverwrite implements CommitFunction {
    private static final DynamicPartitionOverwrite INSTANCE = new DynamicPartitionOverwrite();

    public static DynamicPartitionOverwrite get() {
      return INSTANCE;
    }

    @Override
    public void apply(Table table, Iterable<DataFile> newFiles) {
      ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

      int numFiles = 0;
      for (DataFile file : newFiles) {
        numFiles += 1;
        dynamicOverwrite.addFile(file);
      }

      LOG.info("Committing dynamic partition overwrite with {} files to table {}", numFiles, table);
      long start = System.currentTimeMillis();
      dynamicOverwrite.commit();
      long duration = System.currentTimeMillis() - start;
      LOG.info("Committed in {} ms", duration);
    }
  }

  public static class Modify implements CommitFunction {
    private final Iterable<DataFile> deletedFiles;
    private final Expression failOnNewFilesExpr;

    private Modify(Iterable<DataFile> deletedFiles, Expression failOnNewFilesExpr) {
      this.deletedFiles = deletedFiles;
      this.failOnNewFilesExpr = failOnNewFilesExpr;
    }

    public static Modify files(Iterable<DataFile> deletedFiles, Expression failOnNewFilesExpr) {
      return new Modify(deletedFiles, failOnNewFilesExpr);
    }

    @Override
    public void apply(Table table, Iterable<DataFile> newFiles) {
      ModifyFiles modifyFiles = table.newModify()
          .modifyFiles(ImmutableSet.copyOf(deletedFiles), ImmutableSet.copyOf(newFiles))
          .failOnNewFiles(failOnNewFilesExpr);

      LOG.info(
          "Committing modify files with {} deleted and {} new files to table {} prohibiting new files where {}",
          Iterables.size(deletedFiles), Iterables.size(newFiles), table, failOnNewFilesExpr);
      long start = System.currentTimeMillis();
      modifyFiles.commit();
      long duration = System.currentTimeMillis() - start;
      LOG.info("Committed in {} ms", duration);
    }
  }

  public static class Rewrite implements CommitFunction {
    private final Iterable<DataFile> deletedFiles;

    private Rewrite(Iterable<DataFile> deletedFiles) {
      this.deletedFiles = deletedFiles;
    }

    public static Rewrite files(Iterable<DataFile> deletedFiles) {
      return new Rewrite(deletedFiles);
    }

    @Override
    public void apply(Table table, Iterable<DataFile> newFiles) {
      RewriteFiles rewriteFiles = table.newRewrite()
              .rewriteFiles(ImmutableSet.copyOf(deletedFiles), ImmutableSet.copyOf(newFiles));

      LOG.info("Committing rewrite files with {} deleted and {} new files to table {}",
              Iterables.size(deletedFiles), Iterables.size(newFiles), table);
      long start = System.currentTimeMillis();
      rewriteFiles.commit();
      long duration = System.currentTimeMillis() - start;
      LOG.info("Committed rewrite in {} ms", duration);
    }
  }

}
