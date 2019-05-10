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

import java.util.List;
import java.util.UUID;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTableUtil.SparkDataFile;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.expr;

public class TestTableMigration {

    @Test
    public void testTableMigration() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        Configuration conf = new Configuration();
        String tmpDir = conf.get("hadoop.tmp.dir");
        UUID tableSuffix = UUID.randomUUID();

        // Generate a location for the source Spark table
        Path sparkTablePath = new Path(tmpDir, "spark-table-" + tableSuffix);
        String sparkTableLocation = sparkTablePath.toString();

        // Generate a location for the destination Iceberg table
        Path icebergTablePath = new Path(tmpDir, "iceberg-table-" + tableSuffix);
        String icebergTableLocation = icebergTablePath.toString();

        // Generate data
        Dataset<Row> df = spark.range(1000000)
                .withColumn("intCol", expr("CAST(id % 20 AS INT)"))
                .withColumn("dateCol", expr("DATE_ADD(CURRENT_DATE(), (id % 20))"))
                .withColumn("stringCol", expr("CAST(dateCol AS STRING)"));

        // Write to the partitioned Spark table via the built-in file source
        df.write()
                .format("parquet")
                .partitionBy("dateCol", "intCol")
                .save(sparkTableLocation);

        // Create the destination Iceberg table
        // Important! Keep the partitioning columns at the end just as the built-in file source
        HadoopTables hadoopTables = new HadoopTables();
        Schema icebergSchema = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "stringCol", Types.StringType.get()),
                optional(4, "dateCol", Types.DateType.get()),
                optional(3, "intCol", Types.IntegerType.get()));

        PartitionSpec partitionSpec = PartitionSpec.builderFor(icebergSchema)
                .identity("dateCol")
                .identity("intCol")
                .build();
        hadoopTables.create(icebergSchema, partitionSpec, icebergTableLocation);

        // Get all files from the source Spark table
        Dataset<SparkDataFile> files = SparkTableUtil.filesDataset(spark, sparkTablePath, "parquet");

        // Make Spark aware that we are using manifest lists in Iceberg
        spark.sparkContext().hadoopConfiguration().set(TableProperties.MANIFEST_LISTS_ENABLED, "true");

        // Append all files to the table without copying them
        files.orderBy("path")
                .coalesce(10)
                .foreachPartition(fileIter -> {
                    HadoopTables tables = new HadoopTables();
                    Table table = tables.load(icebergTableLocation);
                    AppendFiles appendFiles = table.newAppend();
                    fileIter.forEachRemaining(file -> appendFiles.appendFile(file.toDataFile(table.spec())));
                    appendFiles.commit();
                });

        // Query the source Spark table
        Dataset<Row> fileSourceDF = spark.read()
                .format("parquet")
                .load(sparkTableLocation);

        List<Row> fileSourceRows = fileSourceDF
                .orderBy("id")
                .collectAsList();

        // Query the Iceberg table that references the data written using Spark
        Dataset<Row> icebergDF = spark.read()
                .format("iceberg")
                .load(icebergTableLocation);

        List<Row> icebergRows = icebergDF
                .select("id", "stringCol", "dateCol", "intCol")
                .orderBy("id")
                .collectAsList();

        Assert.assertEquals(fileSourceRows, icebergRows);
    }
}