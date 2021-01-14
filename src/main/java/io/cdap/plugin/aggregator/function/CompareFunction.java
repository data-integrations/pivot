/*
 * Copyright © 2021 Cask Data, Inc.
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

/**
 * Base function to compare two values
 *
 * @param <T> type of aggregate value
 * @param <V> type of aggregate function
 */
public abstract class CompareFunction<T, V extends CompareFunction<T, V>> implements AggregateFunction<T, V> {
  protected final String fieldName;
  protected final Schema fieldSchema;
  protected final Schema.Type fieldType;
  protected T result;

  public CompareFunction(final String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
    this.fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
  }

  @Override
  public void initialize() {
    this.result = null;
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    result = compare(result, record.get(fieldName));
  }

  @Override
  public void mergeAggregates(CompareFunction otherAgg) {
    result = compare(result, otherAgg.getAggregate());
  }

  @Override
  public T getAggregate() {
    return result;
  }

  @Override
  public Schema getOutputSchema() {
    return fieldSchema;
  }

  public abstract T compare(Object value, Object otherValue);
}
