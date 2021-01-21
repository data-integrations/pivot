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


import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.aggregator.function.AggregateFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Deduplicate aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Pivot")
@Description("Transpose specific rows as columns, and generate aggregate results in those columns")
public class Pivot extends BatchReducibleAggregator<StructuredRecord, StructuredRecord, AggregateResult,
  StructuredRecord> {
  private final PivotConfig config;

  @Nullable
  private final Integer numPartitions;

  private Schema outputSchema;
  private List<PivotConfig.FunctionInfo> functionInfos;
  private Map<String, Set<String>> pivotColumnsAndData;
  //keep ordered columns name, to generate always same key from columns data models
  private ArrayList<String> orderedColumnsName;
  private List<String> columnsProduct;

  private Map<String, String> fieldAliases;
  private FailureCollector failureCollector;


  public Pivot(PivotConfig config, @Nullable Integer numPartitions) {
    this.config = config;
    this.numPartitions = numPartitions;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    failureCollector = stageConfigurer.getFailureCollector();

    Schema inputSchema = stageConfigurer.getInputSchema();

    config.validate(inputSchema, failureCollector);
    if (inputSchema == null || config.containsMacro()) {
      stageConfigurer.setOutputSchema(null);
      return;
    }

    init(failureCollector);
    outputSchema = generateOutputSchema(inputSchema, config.getPivotRows(), functionInfos, columnsProduct);
    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    super.prepareRun(context);
    if (numPartitions != null) {
      context.setNumPartitions(numPartitions);
    }
    if (context.getInputSchema() != null) {
      init(context.getFailureCollector());
      config.validate(context.getInputSchema(), failureCollector);
    }
    recordLineage(context);
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    init(context.getFailureCollector());
    if (context.getInputSchema() != null) {
      outputSchema = generateOutputSchema(context.getInputSchema(), config.getPivotRows(),
                                          functionInfos, columnsProduct);
    }
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) {
    Schema schema = getGroupKeySchema(record.getSchema());
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (String groupByField : config.getPivotRows()) {
      builder.set(groupByField, record.get(groupByField));
    }
    emitter.emit(builder.build());
  }

  private Schema getGroupKeySchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    for (String groupByField :config.getPivotRows()) {
      Schema.Field fieldSchema = inputSchema.getField(groupByField);
      if (fieldSchema == null) {
        throw new IllegalArgumentException(String.format(
          "Cannot group by field '%s' because it does not exist in input schema %s",
          groupByField, inputSchema));
      }
      fields.add(fieldSchema);
    }
    return Schema.recordOf("group.pivot.schema", fields);
  }

  @Override
  public AggregateResult initializeAggregateValue(StructuredRecord record) {
    Map<String, AggregateFunction> map = new HashMap<>();

    for (PivotConfig.FunctionInfo functionInfo : functionInfos) {
      for (String column : columnsProduct) {
        String columnAndFunction = String.format("%s_%s", column, getFunctionName(functionInfo));
        Schema.Field inputField = record.getSchema().getField(functionInfo.getField());
        Schema fieldSchema = inputField == null ? null : inputField.getSchema();
        AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(fieldSchema);
        aggregateFunction.initialize();
        map.put(columnAndFunction, aggregateFunction);
      }
    }

    AggregateResult aggregateResult = new AggregateResult(record.getSchema(), map);
    updateAggregates(aggregateResult.getFunctions(), aggregateResult.getMissingColumnsModel(), record);
    return aggregateResult;
  }

  @Override
  public AggregateResult mergeValues(AggregateResult aggregateResult, StructuredRecord structuredRecord) {
    updateAggregates(aggregateResult.getFunctions(), aggregateResult.getMissingColumnsModel(), structuredRecord);
    return aggregateResult;
  }

  private void updateAggregates(Map<String, AggregateFunction> aggregateFunctions,
                                HashMap<String, Set<Object>> missingColumnsModel, StructuredRecord structuredRecord) {
    String columnKey = generateKeyFromColumnsName(orderedColumnsName, structuredRecord, missingColumnsModel);
    if (columnKey == null) {
      return;
    }
    for (PivotConfig.FunctionInfo aggregate : functionInfos) {
      String columnAndFunction = String.format("%s_%s", columnKey, getFunctionName(aggregate));
      AggregateFunction aggregateFunction = aggregateFunctions.get(columnAndFunction);
      aggregateFunction.mergeValue(structuredRecord);
    }
  }

  @Override
  public AggregateResult mergePartitions(AggregateResult aggregateResult1, AggregateResult aggregateResult2) {
    Map<String, AggregateFunction> functions1 = aggregateResult1.getFunctions();
    Map<String, AggregateFunction> functions2 = aggregateResult2.getFunctions();

    for (Map.Entry<String, AggregateFunction> aggregateFunction : functions1.entrySet()) {
      aggregateFunction.getValue().mergeAggregates(functions2.get(aggregateFunction.getKey()));
    }

    aggregateResult1.mergeMissingColumnDataModels(aggregateResult2);
    return aggregateResult1;
  }

  @Override
  public void finalize(StructuredRecord structuredRecord, AggregateResult groupValue, Emitter<StructuredRecord> emitter)
    throws Exception {
    if (outputSchema == null) {
      config.validate(groupValue.getInputSchema(), failureCollector);
      outputSchema = generateOutputSchema(groupValue.getInputSchema(), config.getPivotRows(),
                                          functionInfos, columnsProduct);
    }

    String onError = config.getOnError();
    if (!PivotConfig.ERROR_SKIP.equals(onError) && groupValue.getMissingColumnsModel().size() > 0) {
      handleOnError(onError, groupValue.getMissingColumnsModel(), emitter, structuredRecord);
      return;
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(this.outputSchema);
    List<Schema.Field> fields = this.outputSchema.getFields();

    Set<String> pivotRows = config.getPivotRows();
    String defaultValue = config.getDefaultValue();

    Map<String, AggregateFunction> functions = groupValue.getFunctions();

    for (Schema.Field field : fields) {
      String fieldName = field.getName();
      if (pivotRows.contains(fieldName)) {
        builder.set(fieldName, structuredRecord.get(fieldName));
      } else {
        String functionKey = findAggregationFunctionsKey(fieldName);
        AggregateFunction aggregateFunction = functions.get(functionKey);
        Object aggregateValue = aggregateFunction.getAggregate();
        if (aggregateValue == null) {
          if (defaultValue != null) {
            builder.convertAndSet(fieldName, config.getDefaultValue());
          }
        } else {
          builder.set(fieldName, aggregateValue);
        }
      }
    }
    emitter.emit(builder.build());
  }

  private void handleOnError(String onError, HashMap<String, Set<Object>> missingColumnsModel,
                             Emitter<StructuredRecord> emitter, StructuredRecord inputRecord) throws Exception {
    String errorMessage = missingColumnsModel.entrySet().stream()
      .map(missingColumnValue -> {
             String missingModels = missingColumnValue.getValue().
               stream()
               .map(String::valueOf)
               .collect(Collectors.joining(" ,"));
             return String.format("For columns name: %s following models are missing %s",
                                  missingColumnValue.getKey(), missingModels);
           }
      ).collect(Collectors.joining(" ;"));

    if (PivotConfig.ERROR_FAIL_PIPELINE.equals(onError)) {
      emitter.emitAlert(ImmutableMap.of("Error", errorMessage));
      throw new Exception(String.format("Failing pipeline due to error : %s", errorMessage));
    }

    if (PivotConfig.ERROR_SEND_TO_ERROR_PORT.equals(onError)) {
      emitter.emitError(new InvalidEntry<>(0, errorMessage, inputRecord));
    }
  }

  private void init(FailureCollector failureCollector) {
    this.failureCollector = failureCollector;
    pivotColumnsAndData = config.getPivotColumnsAndData(failureCollector);
    orderedColumnsName = new ArrayList<>(pivotColumnsAndData.keySet());
    functionInfos = config.getAggregates(failureCollector);
    fieldAliases = config.getFieldAliases(failureCollector);
    columnsProduct = generateColumnsProduct(pivotColumnsAndData);
    failureCollector.getOrThrowException();
  }

  public Schema generateOutputSchema(Schema inputSchema, Iterable<String> pivotRows,
                                     List<PivotConfig.FunctionInfo> aggregates, List<String> columnProducts) {
    List<Schema.Field> fields = new ArrayList<>();

    for (String pivotRow : pivotRows) {
      Schema pivotRowSchema = inputSchema.getField(pivotRow).getSchema();
      fields.add(Schema.Field.of(pivotRow, pivotRowSchema));
    }

    for (PivotConfig.FunctionInfo functionInfo : aggregates) {
      for (String column : columnProducts) {
        String fieldName = String.format("%s_%s", column, getFunctionName(functionInfo));
        String fieldAlias = getFieldAlias(fieldName);
        fieldName = Strings.isNullOrEmpty(fieldAlias) ? fieldName : fieldAlias;

        Schema.Field inputField = inputSchema.getField(functionInfo.getField());
        Schema fieldSchema = inputField == null ? null : inputField.getSchema();
        Schema outputSchema = functionInfo.getAggregateFunction(fieldSchema).getOutputSchema();
        outputSchema = outputSchema.isNullable() ? outputSchema : Schema.nullableOf(outputSchema);

        Schema.Field outputFieldSchema = Schema.Field.of(fieldName, outputSchema);
        fields.add(outputFieldSchema);
      }
    }
    return Schema.recordOf("pivot.output", fields);
  }

  private void recordLineage(BatchAggregatorContext context) {
    Schema inputSchema = context.getInputSchema();
    if (inputSchema == null) {
      return;
    }
    init(context.getFailureCollector());
    Set<String> pivotRows = config.getPivotRows();
    String name = String.format("Pivot %s", String.join(", ", pivotRows));
    List<String> inputFields = new ArrayList<>(orderedColumnsName);
    inputFields.addAll(pivotRows);

    outputSchema = generateOutputSchema(inputSchema, pivotRows, functionInfos, columnsProduct);
    List<String> outputFields = outputSchema.getFields()
      .stream().map(Schema.Field::getName).collect(Collectors.toList());
    String aggregates = functionInfos.stream().map(functionInfo -> String.format("%s(%s)", functionInfo.getName(),
                                                                                 functionInfo.getField()))
      .collect(Collectors.joining(", "));
    String description = String.format("Pivoted the dataset by using the input field(s) %s as the pivot row, and the " +
                                         "fields %s ​as the pivot columns with %s ​as the aggregate function(s) to" +
                                         " generate the fields %s​.",
                                       String.join(", ", pivotRows),
                                       orderedColumnsName.stream().collect(Collectors.joining(" and ")),
                                       aggregates,
                                       outputFields.stream().collect(Collectors.joining(","))
    );
    FieldOperation operation = new FieldTransformOperation(name, description, inputFields, outputFields);
    context.record(Collections.singletonList(operation));
  }

  private String generateKeyFromColumnsName(List<String> pivotColumnNames, StructuredRecord record,
                                            HashMap<String, Set<Object>> missingColumnDataModels) {
    String key = null;

    for (String columnName : pivotColumnNames) {
      Object value = record.get(columnName);
      Set<String> dataModels = pivotColumnsAndData.get(columnName);
      if (!dataModels.contains(value)) {
        Set<Object> missingDataModels = missingColumnDataModels.get(columnName);
        if (missingDataModels == null) {
          missingDataModels = new HashSet<>();
          missingColumnDataModels.put(columnName, missingDataModels);
        }
        missingDataModels.add(value);
        return null;
      }
      if (key == null) {
        key = String.valueOf(value);
      } else {
        key = String.format("%s_%s", key, value);
      }
    }
    return key;
  }

  /**
   * Product of column data models
   * example:
   * Column1: A1,A2
   * Column2: B1
   * result: A1_B1, A2_B1
   *
   * @param pivotColumnsAndData data model for every pivot columns
   * @return List of product result
   */
  public List<String> generateColumnsProduct(Map<String, Set<String>> pivotColumnsAndData) {
    Collection<Set<String>> values = pivotColumnsAndData.values();
    Set<List<String>> productResult = Sets.cartesianProduct(Lists.newArrayList(values));
    return productResult.stream()
      .map(strings -> strings.stream().reduce((s, s2) -> String.format("%s_%s", s, s2))
        .orElse(""))
      .collect(Collectors.toList());
  }

  private String getFunctionName(PivotConfig.FunctionInfo functionInfo) {
    return Strings.isNullOrEmpty(functionInfo.getName()) ? functionInfo.getFunction().name() : functionInfo.getName();
  }

  private String getFieldAlias(String columnName) {
    return fieldAliases == null ? null : fieldAliases.get(columnName);
  }

  private String findAggregationFunctionsKey(String alias) {
    if (fieldAliases == null) {
      return alias;
    }
    for (Map.Entry<String, String> fieldNameAliasEntry : fieldAliases.entrySet()) {
      if (fieldNameAliasEntry.getValue().equals(alias)) {
        return fieldNameAliasEntry.getKey();
      }
    }
    return alias;
  }
}
