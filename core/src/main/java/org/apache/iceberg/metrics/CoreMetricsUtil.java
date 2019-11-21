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

package org.apache.iceberg.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import java.util.Locale;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;

public class CoreMetricsUtil {

  private CoreMetricsUtil() {
  }

  private static final MetricRegistry metricRegistry = new MetricRegistry();

  public static MetricRegistry metricRegistry() {
    return metricRegistry;
  }

//  The top level parent metric registry (in the case of Spark,
//  it will be org.apache.spark.metrics.MetricSystem.registry) will register all the available
//  metrics at the time of registration. Any metrics those are added to the child MetricRegistry
//  will not propagate to parent metric registry.
//  This is a problem (https://github.com/dropwizard/metrics/issues/1214)
//  and is solved in dropwizard metrics v4.1.1 (https://github.com/dropwizard/metrics/pull/1393)
//  which provides child-aware MetricRegistry registration. Until this is used, we will
//  have to prime the child metric registries with metrics before using them.

  static {
    MeteredTableOperations.preRegisterMetrics();
  }

  public static Timer timer(String type, String metric) {
    return metricRegistry.timer(MetricRegistry.name(type, metric));
  }

  public static TableOperations wrapWithMeterIfConfigured(Configuration conf, String name, TableOperations delegate) {
    return wrapWithMeterIfConfigured(
        conf,
        delegate,
        t -> new MeteredTableOperations(metricRegistry, TableOperationsMetricType.from(name), t));
  }

  private static <T> T wrapWithMeterIfConfigured(Configuration conf, T delegate, Function<T, T> wrapperFunc) {
    Preconditions.checkArgument(conf != null, "Configuration is null");

    if (conf.getBoolean("iceberg.dropwizard.enable-metrics-collection", false)) {
      return wrapperFunc.apply(delegate);
    } else {
      return delegate;
    }
  }

  public enum TableOperationsMetricType {
    HIVE("hive"),
    HADOOP("hadoop");

    private final String prefix;

    public String prefix() {
      return prefix;
    }

    TableOperationsMetricType(String prefix) {
      this.prefix = prefix;
    }

    public static TableOperationsMetricType from(String type) {
      Preconditions.checkArgument(type != null, "TableOperations Metric Type is null");
      return TableOperationsMetricType.valueOf(type.toUpperCase(Locale.ENGLISH));
    }
  }
}
