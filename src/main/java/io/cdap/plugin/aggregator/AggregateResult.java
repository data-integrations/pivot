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

package io.cdap.plugin.aggregator;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.aggregator.function.AggregateFunction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A class which represents the aggregation result of a group by aggregator.
 * This class is needed to have the schema since we don't have schema propagation in prepareRun if
 * schema is macro-enabled
 * Holds data models that are not defined in config for every columns, so in finalize we can emmit error if needed
 */
public class AggregateResult implements Serializable {
  private final Schema inputSchema;
  private final Map<String, AggregateFunction> functions;
  private final HashMap<String, Set<Object>> missingColumnsModel;

  public AggregateResult(Schema inputSchema, Map<String, AggregateFunction> functions) {
    this.inputSchema = inputSchema;
    this.functions = functions;
    missingColumnsModel = new HashMap<>();
  }

  public Schema getInputSchema() {
    return inputSchema;
  }

  public Map<String, AggregateFunction> getFunctions() {
    return functions;
  }

  public HashMap<String, Set<Object>> getMissingColumnsModel() {
    return missingColumnsModel;
  }

  public void mergeMissingColumnDataModels(AggregateResult otherResult) {
    missingColumnsModel.putAll(otherResult.getMissingColumnsModel());
  }

}
