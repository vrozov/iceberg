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

import org.apache.spark.sql.SparkSession

class IcebergActions(spark: SparkSession, table: Table) {

  def rewriteManifests[K](strategy: RewriteManifestStrategy[K])(implicit i: K => Ordered[K]): Unit = {
    val action = RewriteManifestsAction[K](strategy)
    action.execute(spark, table)
  }
}

object IcebergActions {

  def forTable(table: Table): IcebergActions = {
    val spark = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("No active SparkSession")
    }
    forTable(spark, table)
  }

  def forTable(spark: SparkSession, table: Table): IcebergActions = {
    new IcebergActions(spark, table)
  }
}
