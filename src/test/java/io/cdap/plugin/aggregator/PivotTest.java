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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Test class for Pivot plugin
 */
public class PivotTest extends HydratorTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactSummary APP_ARTIFACT_PIPELINE =
    new ArtifactSummary("data-pipeline", "1.0.0");

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifactPipeline =
      NamespaceId.DEFAULT.artifact(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion());

    setupBatchArtifacts(parentArtifactPipeline, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("pivot", "1.0.0"),
                      parentArtifactPipeline, Pivot.class);
  }

  private static final Schema INPUT_SCHEMA = Schema.recordOf(
    "purchase",
    Schema.Field.of("Quarter", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Product", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Brand", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Sales", Schema.of(Schema.Type.INT)),
    Schema.Field.of("ShopID", Schema.of(Schema.Type.INT)));

  List<StructuredRecord> inputData = ImmutableList.of(
    //q1
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q1").set("Product", "Shoes").set("Brand", "Nike")
      .set("Sales", 50).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q1").set("Product", "Shirts").set("Brand", "Nike")
      .set("Sales", 20).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q1").set("Product", "Socks").set("Brand", "Reebok")
      .set("Sales", 40).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q1").set("Product", "Shirts").set("Brand", "Reebok")
      .set("Sales", 60).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q1").set("Product", "Shoes").set("Brand", "Reebok")
      .set("Sales", 50).set("ShopID", 1).build(),

    //q2
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q2").set("Product", "Shoes").set("Brand", "Nike")
      .set("Sales", 20).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q2").set("Product", "Shoes").set("Brand", "Reebok")
      .set("Sales", 30).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q2").set("Product", "Socks").set("Brand", "Nike")
      .set("Sales", 40).set("ShopID", 1).build(),


    //q3
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q3").set("Product", "Shoes").set("Brand", "Nike")
      .set("Sales", 50).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q3").set("Product", "Shoes").set("Brand", "Reebok")
      .set("Sales", 30).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q3").set("Product", "Socks").set("Brand", "Reebok")
      .set("Sales", 40).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q3").set("Product", "Socks").set("Brand", "Nike")
      .set("Sales", 20).set("ShopID", 1).build(),

    //q4
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q4").set("Product", "Shoes").set("Brand", "Reebok")
      .set("Sales", 10).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q4").set("Product", "Shirts").set("Brand", "Reebok")
      .set("Sales", 20).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q4").set("Product", "Socks").set("Brand", "Reebok")
      .set("Sales", 30).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q4").set("Product", "Shoes").set("Brand", "Nike")
      .set("Sales", 40).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q4").set("Product", "Shirts").set("Brand", "Nike")
      .set("Sales", 50).set("ShopID", 1).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("Quarter", "Q4").set("Product", "Socks").set("Brand", "Nike")
      .set("Sales", 60).set("ShopID", 1).build()
  );

  // SUM By Product per quarter
  private static final Schema EXPECTED_SUM_BY_PRODUCT_PER_QUARTER_SCHEMA = Schema.recordOf(
    "sumbyproductperquarter",
    Schema.Field.of("Product", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Q1_sum", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("Q2_sum", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("Q3_sum", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("Q4_sum", Schema.nullableOf(Schema.of(Schema.Type.INT)))
  );

  List<StructuredRecord> expectedSumOfSalesByProductPerQuarter = ImmutableList.of(
    StructuredRecord.builder(EXPECTED_SUM_BY_PRODUCT_PER_QUARTER_SCHEMA)
      .set("Product", "Shoes")
      .set("Q1_sum", 100)
      .set("Q2_sum", 50)
      .set("Q3_sum", 80)
      .set("Q4_sum", 50)
      .build(),
    StructuredRecord.builder(EXPECTED_SUM_BY_PRODUCT_PER_QUARTER_SCHEMA)
      .set("Product", "Socks")
      .set("Q1_sum", 40)
      .set("Q2_sum", 40)
      .set("Q3_sum", 60)
      .set("Q4_sum", 90)
      .build(),
    StructuredRecord.builder(EXPECTED_SUM_BY_PRODUCT_PER_QUARTER_SCHEMA)
      .set("Product", "Shirts")
      .set("Q1_sum", 80)
      .set("Q2_sum", null)
      .set("Q3_sum", null)
      .set("Q4_sum", 70)
      .build()
  );

  // SUM By Brand per quarter
  private static final Schema EXPECTED_SUM_BY_BRAND_PER_QUARTER_SCHEMA = Schema.recordOf(
    "sumbyproductperquarter",
    Schema.Field.of("Brand", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Q1_total", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("Q2_total", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("Q3_total", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("Q4_total", Schema.nullableOf(Schema.of(Schema.Type.INT)))
  );

  List<StructuredRecord> expectedSumOfSalesByBrandPerQuarter = ImmutableList.of(
    StructuredRecord.builder(EXPECTED_SUM_BY_BRAND_PER_QUARTER_SCHEMA)
      .set("Brand", "Nike")
      .set("Q1_total", 70)
      .set("Q2_total", 60)
      .set("Q3_total", 70)
      .set("Q4_total", 150)
      .build(),
    StructuredRecord.builder(EXPECTED_SUM_BY_BRAND_PER_QUARTER_SCHEMA)
      .set("Brand", "Reebok")
      .set("Q1_total", 150)
      .set("Q2_total", 30)
      .set("Q3_total", 70)
      .set("Q4_total", 60)
      .build()
  );

  // SUM By Product per quarter
  private static final Schema EXPECTED_SUM_BY_PRODUCT_PER_QUARTER_WITH_ALIAS_SCHEMA = Schema.recordOf(
    "sumbyproductperquarterwithalias",
    Schema.Field.of("Product", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Quarter_1_sum", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("Quarter_2_sum", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("Q3_sum", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("Q4_sum", Schema.nullableOf(Schema.of(Schema.Type.INT)))
  );
  List<StructuredRecord> expectedSumOfSalesByProductPerQuarterWithDefaults = ImmutableList.of(
    StructuredRecord.builder(EXPECTED_SUM_BY_PRODUCT_PER_QUARTER_WITH_ALIAS_SCHEMA)
      .set("Product", "Shoes")
      .set("Quarter_1_sum", 100)
      .set("Quarter_2_sum", 50)
      .set("Q3_sum", 80)
      .set("Q4_sum", 50)
      .build(),
    StructuredRecord.builder(EXPECTED_SUM_BY_PRODUCT_PER_QUARTER_WITH_ALIAS_SCHEMA)
      .set("Product", "Socks")
      .set("Quarter_1_sum", 40)
      .set("Quarter_2_sum", 40)
      .set("Q3_sum", 60)
      .set("Q4_sum", 90)
      .build(),
    StructuredRecord.builder(EXPECTED_SUM_BY_PRODUCT_PER_QUARTER_WITH_ALIAS_SCHEMA)
      .set("Product", "Shirts")
      .set("Quarter_1_sum", 80)
      .set("Quarter_2_sum", 0)
      .set("Q3_sum", 0)
      .set("Q4_sum", 70)
      .build()
  );

  // MIN/MAX with String value per quarter
  private static final Schema EXPECTED_MIN_MAX_BRAND_PER_QUARTER_SCHEMA = Schema.recordOf(
    "minmaxproductperquarter",
    Schema.Field.of("Product", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Q1_min", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Q2_min", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Q3_min", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Q4_min", Schema.nullableOf(Schema.of(Schema.Type.STRING))),

    Schema.Field.of("Q1_max", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Q2_max", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Q3_max", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Q4_max", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
  );

  List<StructuredRecord> expectedMinMaxProductPerQuarter = ImmutableList.of(
    StructuredRecord.builder(EXPECTED_MIN_MAX_BRAND_PER_QUARTER_SCHEMA)
      .set("Product", "Shirts")
      .set("Q1_min", "Nike")
      .set("Q2_min", null)
      .set("Q3_min", null)
      .set("Q4_min", "Nike")
      .set("Q1_max", "Reebok")
      .set("Q2_max", null)
      .set("Q3_max", null)
      .set("Q4_max", "Reebok")
      .build(),
    StructuredRecord.builder(EXPECTED_MIN_MAX_BRAND_PER_QUARTER_SCHEMA)
      .set("Product", "Shoes")
      .set("Q1_min", "Nike")
      .set("Q2_min", "Nike")
      .set("Q3_min", "Nike")
      .set("Q4_min", "Nike")
      .set("Q1_max", "Reebok")
      .set("Q2_max", "Reebok")
      .set("Q3_max", "Reebok")
      .set("Q4_max", "Reebok")
      .build(),
    StructuredRecord.builder(EXPECTED_MIN_MAX_BRAND_PER_QUARTER_SCHEMA)
      .set("Product", "Socks")
      .set("Q1_min", "Reebok")
      .set("Q2_min", "Nike")
      .set("Q3_min", "Nike")
      .set("Q4_min", "Nike")
      .set("Q1_max", "Reebok")
      .set("Q2_max", "Nike")
      .set("Q3_max", "Reebok")
      .set("Q4_max", "Reebok")
      .build()
  );

  private List<StructuredRecord> stageSetup(Map<String, String> properties) throws Exception {
    String inputDataset = UUID.randomUUID().toString();
    String outputDateset = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(inputDataset, INPUT_SCHEMA)))
      .addStage(new ETLStage("pivot", new ETLPlugin("Pivot",
                                                    BatchAggregator.PLUGIN_TYPE, properties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputDateset)))
      .addConnection("source", "pivot")
      .addConnection("pivot", "sink")
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion()), config);
    ApplicationId appId = NamespaceId.DEFAULT.app("pivot");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    DataSetManager<Table> inputManager = getDataset(inputDataset);
    MockSource.writeInput(inputManager, inputData);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDateset);
    return MockSink.readOutput(outputManager);
  }

  /**
   * Converts list of {@link StructuredRecord} to list of json strings
   *
   * @param structuredRecords {@link List<StructuredRecord>}
   * @return {@link List<String>}
   */
  private List<String> convertStructuredRecordListToJson(List<StructuredRecord> structuredRecords) {
    return structuredRecords.stream().map(s -> {
      try {
        return StructuredRecordStringConverter.toJsonString(s);
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
      return null;
    }).sorted().collect(Collectors.toList());
  }

  @Test
  public void testSalesSumByProductPerQuarter() throws Exception {

    // Config props
    Map<String, String> properties = new HashMap<>();
    properties.put("pivotColumns", "Quarter=Q1,Q2,Q3,Q4");
    properties.put("pivotRow", "Product");
    properties.put("aggregates", "sum: sum(Sales)");

    // Run pipeline
    List<StructuredRecord> output = stageSetup(properties);

    Assert.assertEquals(expectedSumOfSalesByProductPerQuarter.size(), output.size());
    Assert.assertEquals(convertStructuredRecordListToJson(expectedSumOfSalesByProductPerQuarter),
                        convertStructuredRecordListToJson(output));
  }

  @Test
  public void testSalesSumByBrandPerQuarter() throws Exception {

    // Config props
    Map<String, String> properties = new HashMap<>();
    properties.put("pivotColumns", "Quarter=Q1,Q2,Q3,Q4");
    properties.put("pivotRow", "Brand");
    properties.put("aggregates", "total: sum(Sales)");

    // Run pipeline
    List<StructuredRecord> output = stageSetup(properties);

    Assert.assertEquals(expectedSumOfSalesByBrandPerQuarter.size(), output.size());
    List<String> expected = convertStructuredRecordListToJson(expectedSumOfSalesByBrandPerQuarter);
    List<String> actual = convertStructuredRecordListToJson(output);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMinMaxFunctionWithStringValue() throws Exception {

    // Config props
    Map<String, String> properties = new HashMap<>();
    properties.put("pivotColumns", "Quarter=Q1,Q2,Q3,Q4");
    properties.put("pivotRow", "Product");
    properties.put("aggregates", "min: min(Brand),max: max(Brand)");

    // Run pipeline
    List<StructuredRecord> output = stageSetup(properties);

    Assert.assertEquals(expectedMinMaxProductPerQuarter.size(), output.size());
    List<String> expected = convertStructuredRecordListToJson(expectedMinMaxProductPerQuarter);
    List<String> actual = convertStructuredRecordListToJson(output);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSalesSumByProductPerQuarterWithDefaultsAndAlias() throws Exception {

    // Config props
    Map<String, String> properties = new HashMap<>();
    properties.put("pivotColumns", "Quarter=Q1,Q2,Q3,Q4");
    properties.put("pivotRow", "Product");
    properties.put("aggregates", "sum: sum(Sales)");
    properties.put("defaultValue", "0");
    properties.put("fieldAliases", "Q1_sum:Quarter_1_sum,Q2_sum:Quarter_2_sum");

    // Run pipeline
    List<StructuredRecord> output = stageSetup(properties);

    Assert.assertEquals(expectedSumOfSalesByProductPerQuarterWithDefaults.size(), output.size());
    Assert.assertEquals(convertStructuredRecordListToJson(expectedSumOfSalesByProductPerQuarterWithDefaults),
                        convertStructuredRecordListToJson(output));
  }

  @Test
  public void testConfigWithInvalidPivotColumns() throws NoSuchFieldException {
    // Config props
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PivotConfig config = new PivotConfig();
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("pivotColumns"),
                         "QuarterQ1,Q2,Q3,Q4");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("pivotRow"),
                         "Product");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("aggregates"),
                         "sum: sum(Sales)");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("defaultValue"),
                         "0");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("fieldAliases"),
                         "Q1_sum:Quarter_1_sum,Q2_sum:Quarter_2_sum");
    try {
      config.validate(INPUT_SCHEMA, mockFailureCollector);
      Assert.fail();
    } catch (Exception exception) {
      Assert.assertEquals(2, mockFailureCollector.getValidationFailures().size());
      Assert.assertEquals(PivotConfig.FIELD_NAME_PIVOT_COLUMNS, mockFailureCollector.getValidationFailures()
        .get(0).getCauses().get(0).getAttribute("stageConfig"));
    }
  }

  @Test
  public void testConfigWithInvalidAliasField() throws NoSuchFieldException {
    // Config props
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PivotConfig config = new PivotConfig();
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("pivotColumns"),
                         "Quarter=Q1,Q2,Q3,Q4");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("pivotRow"),
                         "Product");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("aggregates"),
                         "sum: sum(Sales)");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("defaultValue"),
                         "0");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("fieldAliases"),
                         "Q1_sumQuarter_1_sum,Q2_sum:Quarter_2_sum");
    try {
      config.validate(INPUT_SCHEMA, mockFailureCollector);
      Assert.fail();
    } catch (Exception exception) {
      Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
      Assert.assertEquals(PivotConfig.FIELD_NAME_FIELD_ALIASES, mockFailureCollector.getValidationFailures()
        .get(0).getCauses().get(0).getAttribute("stageConfig"));
    }
  }

  @Test
  public void testConfigWithInvalidAggregateField() throws NoSuchFieldException {
    // Config props
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PivotConfig config = new PivotConfig();
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("pivotColumns"),
                         "Quarter=Q1,Q2,Q3,Q4");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("pivotRow"),
                         "Product");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("aggregates"),
                         "sum: sum()");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("defaultValue"),
                         "0");
    FieldSetter.setField(config, PivotConfig.class.getDeclaredField("fieldAliases"),
                         "Q1_sum:Quarter_1_sum,Q2_sum:Quarter_2_sum");
    try {
      config.validate(INPUT_SCHEMA, mockFailureCollector);
      Assert.fail();
    } catch (Exception exception) {
      Assert.assertEquals(2, mockFailureCollector.getValidationFailures().size());
      Assert.assertEquals(PivotConfig.FIELD_NAME_AGGREGATES, mockFailureCollector.getValidationFailures()
        .get(0).getCauses().get(0).getAttribute("stageConfig"));
    }
  }
}
