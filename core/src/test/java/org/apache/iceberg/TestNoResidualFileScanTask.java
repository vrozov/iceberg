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
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Assert;
import org.junit.Test;

public class TestNoResidualFileScanTask extends TableTestBase {

  private static final DataFile FILE = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(2)
      .build();

  @Test
  public void testResidualIsIgnored() throws IOException {
    table.newAppend().appendFile(FILE).commit();

    TableScan scan = table.newScan().filter(Expressions.lessThan("id", 10));

    List<FileScanTask> noResidualTasks = Lists.newArrayList();
    try (CloseableIterable<CombinedScanTask> combinedTasks = scan.planTasks()) {
      combinedTasks.forEach(task -> {
        task.files().forEach(file -> noResidualTasks.add(new NoResidualFileScanTask(file)));
      });
    }

    Assert.assertFalse("Tasks should not be empty", noResidualTasks.isEmpty());
    Assert.assertTrue(
        "Residual expression should be alwaysTrue",
        noResidualTasks.stream().allMatch(t -> t.residual() == Expressions.alwaysTrue()));
  }
}
