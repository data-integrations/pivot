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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.aggregator.function.AggregateFunction;
import io.cdap.plugin.aggregator.function.Avg;
import io.cdap.plugin.aggregator.function.CollectList;
import io.cdap.plugin.aggregator.function.CollectSet;
import io.cdap.plugin.aggregator.function.Concat;
import io.cdap.plugin.aggregator.function.ConcatDistinct;
import io.cdap.plugin.aggregator.function.CorrectedSumOfSquares;
import io.cdap.plugin.aggregator.function.Count;
import io.cdap.plugin.aggregator.function.CountAll;
import io.cdap.plugin.aggregator.function.CountDistinct;
import io.cdap.plugin.aggregator.function.CountNulls;
import io.cdap.plugin.aggregator.function.First;
import io.cdap.plugin.aggregator.function.Last;
import io.cdap.plugin.aggregator.function.LogicalAnd;
import io.cdap.plugin.aggregator.function.LogicalOr;
import io.cdap.plugin.aggregator.function.LongestString;
import io.cdap.plugin.aggregator.function.Max;
import io.cdap.plugin.aggregator.function.Min;
import io.cdap.plugin.aggregator.function.ShortestString;
import io.cdap.plugin.aggregator.function.Stddev;
import io.cdap.plugin.aggregator.function.Sum;
import io.cdap.plugin.aggregator.function.SumOfSquares;
import io.cdap.plugin.aggregator.function.Variance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Pivot Plugin Configuration.
 */
public class PivotConfig extends PluginConfig {

  public static final String FIELD_NAME_PIVOT_COLUMNS = "pivotColumns";
  public static final String FIELD_NAME_PIVOT_ROW = "pivotRow";
  public static final String FIELD_NAME_AGGREGATES = "aggregates";
  public static final String FIELD_NAME_NUM_PARTITION = "numPartitions";
  public static final String FIELD_NAME_DEFAULT_VALUE = "defaultValue";
  public static final String FIELD_NAME_FIELD_ALIASES = "fieldAliases";
  public static final String FIELD_NAME_ON_ERROR = "on-error";

  public static final String ERROR_SKIP = "skip-error";
  public static final String ERROR_SEND_TO_ERROR_PORT = "send-to-error-port";
  public static final String ERROR_FAIL_PIPELINE = "skip-error";

  @Macro
  @Name(FIELD_NAME_PIVOT_COLUMNS)
  @Description("Specifies a list of fields pivot as columns. Usually, these fields contain a fixed set of values, " +
    "which are converted into columns.When a single field is selected, the number of columns added is equal to " +
    "the number of unique values in the field multiplied by the number of aggregates. When multiple fields are " +
    "selected, the number of columns is the product of unique values in each column multiplied by the number " +
    "of aggregates.")
  public String pivotColumns;

  @Macro
  @Name(FIELD_NAME_PIVOT_ROW)
  @Description("Specifies fields in the input schema. The unique values in this field will become the " +
    "rows of the output from the Pivot transform.")
  public String pivotRow;

  @Macro
  @Name(FIELD_NAME_AGGREGATES)
  @Description("Specifies a set of aggregate functions to be applied to the input data for each group (pivot rows)." +
    " A function has the syntax alias:function_name(column). Should specify at least 1 aggregate function.")
  private String aggregates;

  @Macro
  @Nullable
  @Name(FIELD_NAME_NUM_PARTITION)
  @Description("Number of partitions to use when aggregating. If not specified, the execution framework " +
    "will decide how many to use.")
  protected Integer numPartitions;

  @Nullable
  @Macro
  @Name(FIELD_NAME_DEFAULT_VALUE)
  @Description("The default value to use in case a cell does not contain a value after pivoting. Defaults to null.")
  private String defaultValue;

  @Nullable
  @Macro
  @Name(FIELD_NAME_FIELD_ALIASES)
  @Description("Sets up aliases for the fields generated by pivoting the data. By default, columns are of the " +
    "form <unique_value_in_pivot_column>_<aggregation_function>_<aggregated_field>.")
  private String fieldAliases;

  @Nullable
  @Macro
  @Name(FIELD_NAME_ON_ERROR)
  @Description("How to handle cases when not all unique values for column are provided. Default is skip record.")
  private String onError;

  public PivotConfig() {
    this.pivotColumns = "";
    this.pivotRow = "";
  }

  public void validate(Schema inputSchema, FailureCollector failureCollector) {
    if (containsMacro()) {
      return;
    }
    Set<String> pivotRows = getPivotRows();

    if (pivotRows.isEmpty()) {
      failureCollector.addFailure("Invalid pivot rows property.", "The 'pivotRows' property must be set.")
        .withConfigProperty(FIELD_NAME_PIVOT_ROW);
    }

    Map<String, Set<String>> pivotColumnsAndData = getPivotColumnsAndData(failureCollector);
    getFieldAliases(failureCollector);
    List<FunctionInfo> functionInfos = getAggregates(failureCollector);


    if (inputSchema == null) {
      failureCollector.getOrThrowException(); //case when input schema is macro
      return;
    }

    ArrayList<String> orderedColumnsName = new ArrayList<>(pivotColumnsAndData.keySet());

    for (String pivotRow : pivotRows) {
      if (inputSchema.getField(pivotRow) == null) {
        failureCollector.addFailure(String.format("Pivot row %s is not in inputSchema.", pivotRow), null)
          .withConfigProperty(FIELD_NAME_PIVOT_ROW);
      }
    }

    if (orderedColumnsName.size() > 2) {
      failureCollector.addFailure("Maximum allowed number of pivot columns is 2.", null)
        .withConfigProperty(FIELD_NAME_PIVOT_COLUMNS);
    }

    for (String columnName : orderedColumnsName) {
      Schema.Field schemaField = inputSchema.getField(columnName);
      if (schemaField == null) {
        failureCollector.addFailure(String.format("Pivot column %s is not in inputSchema.", columnName), null)
          .withConfigProperty(FIELD_NAME_PIVOT_COLUMNS);
      }
    }

    for (PivotConfig.FunctionInfo functionInfo : functionInfos) {
      String fieldName = functionInfo.getField();
      Schema.Field field = inputSchema.getField(fieldName);
      if (field == null) {
        failureCollector.addFailure(String.format("Field name %s  for function %s does not exist in input schema.",
                                                  fieldName, functionInfo.getName()), null)
          .withConfigProperty(FIELD_NAME_AGGREGATES);
      }
    }
    failureCollector.getOrThrowException();
  }

  public Set<String> getPivotRows() {
    return Strings.isNullOrEmpty(pivotRow) ? Collections.emptySet() : Sets.newHashSet(Splitter.on(",").trimResults()
                                                                                        .split(pivotRow));
  }

  /**
   * @return pivot columns and its data models. Returns an empty list if field contains a macro. Otherwise, the list
   * returned can never be empty.
   */
  public Map<String, Set<String>> getPivotColumnsAndData(FailureCollector failureCollector) {
    Map<String, Set<String>> result = new LinkedHashMap<>();
    if (containsMacro(FIELD_NAME_PIVOT_COLUMNS)) {
      return result;
    }

    Iterable<String> columnsAndDataModel = Splitter.on(";").trimResults().split(pivotColumns);
    for (String columnAndDataModel : columnsAndDataModel) {
      if (!columnAndDataModel.contains("=")) {
        failureCollector.addFailure("Could not find '=' separating column name from its data model.",
                                    "Format should be 'columnName=dataModel1,dataModel2.")
          .withConfigProperty(FIELD_NAME_PIVOT_COLUMNS);
        continue;
      }

      Iterator<String> iterator = Splitter.on("=").trimResults().split(columnAndDataModel).iterator();
      if (!iterator.hasNext()) {
        failureCollector.addFailure("Could not find column name.",
                                    "Format should be 'columnName=dataModel1, dataModel2'.")
          .withConfigProperty(FIELD_NAME_PIVOT_COLUMNS);
        continue;
      }

      String columnName = iterator.next();
      if (result.containsKey(columnName)) {
        failureCollector.addFailure(String.format("Pivot column already defined %s", columnName),
                                    "Column names must be unique.").
          withConfigProperty(FIELD_NAME_PIVOT_COLUMNS);
        continue;
      }

      if (!iterator.hasNext()) {
        failureCollector.addFailure("Could not find data models.",
                                    "Format should be 'columnName=dataModel1,dataModel2").
          withConfigProperty(FIELD_NAME_PIVOT_COLUMNS);
        continue;
      }

      String dataModelsString = iterator.next();
      HashSet<String> dataModels = new LinkedHashSet<>();

      for (String dataModel : Splitter.on(",").trimResults().split(dataModelsString)) {
        if (dataModels.contains(dataModel)) {
          failureCollector.addFailure(
            String.format("Data model %s already defined for pivot column %s.", dataModel, columnName),
            "Values must be unique.")
            .withConfigProperty(FIELD_NAME_PIVOT_COLUMNS);
        } else {
          dataModels.add(dataModel);
        }
      }

      if (dataModels.isEmpty()) {
        failureCollector.addFailure(
          String.format("No data model defined for pivot column %s.", columnName),
          "Add at lease one data model for column.").withConfigProperty(FIELD_NAME_PIVOT_COLUMNS);
      }
      result.put(columnName, dataModels);
    }

    if (result.isEmpty()) {
      failureCollector.addFailure("Select at least one pivot column.",
                                  "Add at lease one data model for column.")
        .withConfigProperty(FIELD_NAME_PIVOT_COLUMNS);
    }
    return result;
  }

  /**
   * @return Map where key is field name generated from product of unique value of columns and aggregation function,
   * and value is alias for that field to be used in output schema/
   */
  @Nullable
  public Map<String, String> getFieldAliases(FailureCollector failureCollector) {
    Map<String, String> result = new HashMap<>();
    if (containsMacro(FIELD_NAME_FIELD_ALIASES) || Strings.isNullOrEmpty(fieldAliases)) {
      return result;
    }

    Iterable<String> iterable = Splitter.on(",").trimResults().split(fieldAliases);

    for (String fieldAndAlias : iterable) {
      if (!fieldAndAlias.contains(":")) {
        failureCollector.addFailure("Could not find ':' separating field name from its alias.",
                                    "Format should be 'fieldName:alias''")
          .withConfigProperty(FIELD_NAME_FIELD_ALIASES);
        continue;
      }
      Iterator<String> iterator = Splitter.on(":").trimResults().split(fieldAndAlias).iterator();
      if (!iterator.hasNext()) {
        failureCollector.addFailure("Could not find field name.", "Format should be 'fieldName:alias''").
          withConfigProperty(FIELD_NAME_FIELD_ALIASES);
        continue;
      }

      String fieldName = iterator.next();
      if (result.containsKey(fieldName)) {
        failureCollector.addFailure(String.format("Field Name already defined %s.", fieldName),
                                    "Field names must be unique.")
          .withConfigProperty(FIELD_NAME_FIELD_ALIASES);
        continue;
      }

      if (!iterator.hasNext()) {
        failureCollector.addFailure(String.format("Could not find alias for %s.", fieldName),
                                    "Format should be 'fieldName:alias''")
          .withConfigProperty(FIELD_NAME_FIELD_ALIASES);
        continue;
      }

      String fieldAlias = iterator.next();
      if (Strings.isNullOrEmpty(fieldAlias)) {
        failureCollector.addFailure(String.format("Could not find alias for %s.", fieldName),
                                    "Format should be 'fieldName:alias''")
          .withConfigProperty(FIELD_NAME_FIELD_ALIASES);
        continue;
      }
      result.put(fieldName, fieldAlias);
    }

    if (result.isEmpty()) {
      failureCollector.addFailure("Could not parse field",
                                  "Format should be 'fieldName1:alias, fieldName2:alias2''")
        .withConfigProperty(FIELD_NAME_FIELD_ALIASES);
    }

    return result;
  }

  /**
   * @return the aggregates to perform. Returns an empty list if aggregates contains a macro. Otherwise, the list
   * returned can never be empty.
   */
  public List<FunctionInfo> getAggregates(FailureCollector collector) {
    List<FunctionInfo> functionInfos = new ArrayList<>();
    if (containsMacro(FIELD_NAME_AGGREGATES)) {
      return functionInfos;
    }
    Set<String> aggregateNames = new HashSet<>();
    for (String aggregate : Splitter.on(',').trimResults().split(aggregates)) {
      int colonIdx = aggregate.indexOf(':');
      if (colonIdx < 0) {
        collector.addFailure(String.format(
          "Could not find ':' separating aggregate name from its function in '%s'.", aggregate), null)
          .withConfigProperty(FIELD_NAME_AGGREGATES);
        continue;
      }
      String name = aggregate.substring(0, colonIdx).trim();
      if (!aggregateNames.add(name)) {
        collector.addFailure(String.format(
          "Cannot create multiple aggregate functions with the same name '%s'.", name), null).
          withConfigProperty(FIELD_NAME_AGGREGATES);
        continue;
      }

      String functionAndField = aggregate.substring(colonIdx + 1).trim();
      int leftParanIdx = functionAndField.indexOf('(');
      if (leftParanIdx < 0) {
        collector.addFailure(String.format(
          "Could not find '(' in function '%s'. Functions must be specified as function(field).",
          functionAndField), null).withConfigProperty(FIELD_NAME_AGGREGATES);
        continue;
      }
      String functionStr = functionAndField.substring(0, leftParanIdx).trim();
      Function function;
      try {
        function = Function.valueOf(functionStr.toUpperCase());
      } catch (IllegalArgumentException e) {
        collector.addFailure(String.format(
          "Invalid function '%s'. Must be one of %s.", functionStr, Joiner.on(',').join(Function.values())), null)
          .withConfigProperty(FIELD_NAME_AGGREGATES);
        continue;
      }

      if (!functionAndField.endsWith(")")) {
        collector.addFailure(String.format(
          "Could not find closing ')' in function '%s'. Functions must be specified as function(field).",
          functionAndField), null).withConfigProperty(FIELD_NAME_AGGREGATES);
        continue;
      }
      String field = functionAndField.substring(leftParanIdx + 1, functionAndField.length() - 1).trim();
      if (field.isEmpty()) {
        collector.addFailure(
          String.format("Invalid function '%s'. A field must be given as an argument.", functionAndField), null)
          .withConfigProperty(FIELD_NAME_AGGREGATES);
        continue;
      }

      functionInfos.add(new FunctionInfo(name, field, function));
    }

    if (functionInfos.isEmpty()) {
      collector.addFailure(String.format("The '%s' property must be set.", FIELD_NAME_AGGREGATES), null)
        .withConfigProperty(FIELD_NAME_AGGREGATES);
      return functionInfos;
    }
    return functionInfos;
  }

  @Nullable
  public String getDefaultValue() {
    return defaultValue;
  }


  public String getOnError() {
    return Strings.isNullOrEmpty(onError) ? ERROR_SKIP : onError;
  }

  public boolean containsMacro() {
    return containsMacro(FIELD_NAME_PIVOT_ROW)
      || containsMacro(FIELD_NAME_PIVOT_COLUMNS)
      || containsMacro(FIELD_NAME_AGGREGATES)
      || containsMacro(FIELD_NAME_DEFAULT_VALUE)
      || containsMacro(FIELD_NAME_FIELD_ALIASES)
      || containsMacro(FIELD_NAME_ON_ERROR);

  }

  /**
   * Class to hold information for an aggregate function.
   */
  static class FunctionInfo {
    private final String name;
    private final String field;
    private final Function function;

    FunctionInfo(String name, String field, Function function) {
      this.name = name;
      this.field = field;
      this.function = function;
    }

    public String getName() {
      return name;
    }

    public String getField() {
      return field;
    }

    public Function getFunction() {
      return function;
    }

    public AggregateFunction getAggregateFunction(Schema fieldSchema) {
      switch (function) {
        case COUNT:
          if ("*".equals(field)) {
            return new CountAll();
          }
          return new Count(field);
        case COUNTDISTINCT:
          return new CountDistinct(field);
        case SUM:
          return new Sum(field, fieldSchema);
        case AVG:
          return new Avg(field, fieldSchema);
        case MIN:
          return new Min(field, fieldSchema);
        case MAX:
          return new Max(field, fieldSchema);
        case FIRST:
          return new First(field, fieldSchema);
        case LAST:
          return new Last(field, fieldSchema);
        case STDDEV:
          return new Stddev(field, fieldSchema);
        case VARIANCE:
          return new Variance(field, fieldSchema);
        case COLLECTLIST:
          return new CollectList(field, fieldSchema);
        case COLLECTSET:
          return new CollectSet(field, fieldSchema);
        case LONGESTSTRING:
          return new LongestString(field, fieldSchema);
        case SHORTESTSTRING:
          return new ShortestString(field, fieldSchema);
        case COUNTNULLS:
          return new CountNulls(field);
        case CONCAT:
          return new Concat(field, fieldSchema);
        case CONCATDISTINCT:
          return new ConcatDistinct(field, fieldSchema);
        case LOGICALAND:
          return new LogicalAnd(field);
        case LOGICALOR:
          return new LogicalOr(field);
        case CORRECTEDSUMOFSQUARES:
          return new CorrectedSumOfSquares(field, fieldSchema);
        case SUMOFSQUARES:
          return new SumOfSquares(field, fieldSchema);
      }
      // should never happen
      throw new IllegalStateException("Unknown function type " + function);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FunctionInfo that = (FunctionInfo) o;

      return Objects.equals(name, that.name) &&
        Objects.equals(field, that.field) &&
        Objects.equals(function, that.function);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, field, function);
    }

    @Override
    public String toString() {
      return "FunctionInfo{" +
        "name='" + name + '\'' +
        ", field='" + field + '\'' +
        ", function=" + function +
        '}';
    }
  }

  /**
   * Aggregate Function
   */
  public enum Function {
    COUNT,
    COUNTDISTINCT,
    SUM,
    AVG,
    MIN,
    MAX,
    FIRST,
    LAST,
    STDDEV,
    VARIANCE,
    COLLECTLIST,
    COLLECTSET,
    LONGESTSTRING,
    SHORTESTSTRING,
    COUNTNULLS,
    CONCAT,
    CONCATDISTINCT,
    LOGICALAND,
    LOGICALOR,
    CORRECTEDSUMOFSQUARES,
    SUMOFSQUARES
  }
}
