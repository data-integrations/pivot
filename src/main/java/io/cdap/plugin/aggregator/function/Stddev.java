/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.aggregator.function;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.aggregator.AggregationUtils;

import javax.annotation.Nullable;

/**
 * Calculates the Standard Deviation
 */
public class Stddev implements AggregateFunction<Double, Stddev> {
  private final Variance variance;

  public Stddev(String fieldName, Schema fieldSchema) {
    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (!AggregationUtils.isNumericType(fieldType, logicalType)) {
      throw new IllegalArgumentException(String.format(
        "Cannot compute standard deviation on field %s because its type %s is not numeric", fieldName,
        fieldSchema.getDisplayName()));
    }
    this.variance = new Variance(fieldName, fieldSchema);
  }

  @Override
  public void initialize() {
    variance.initialize();
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    variance.mergeValue(record);
  }

  @Override
  public void mergeAggregates(Stddev otherAgg) {
    variance.mergeAggregates(otherAgg.variance);
  }

  @Nullable
  @Override
  public Double getAggregate() {
    Double aggregate = variance.getAggregate();
    if (aggregate == null) {
      return null;
    }
    return Math.sqrt(aggregate);
  }

  @Override
  public Schema getOutputSchema() {
    return variance.getOutputSchema();
  }
}
