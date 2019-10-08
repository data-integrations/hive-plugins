/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.huve.action;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.hive.action.common.HiveConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HiveConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final HiveConfig VALID_CONFIG = new HiveConfig(
    "jdbc:hive2://localhost:10000/mydb;auth=delegationToken",
    "user",
    "password",
    "LOAD DATA INPATH '/tmp/hive' INTO TABLE testTable"
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    VALID_CONFIG.validateImportStatement(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateIncorrectConnectionString() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HiveConfig config = HiveConfig.builder(VALID_CONFIG)
      .setConnectionString("hive2://localhost:10000/mydb;auth=delegationToken")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(HiveConfig.CONNECTION_STRING));

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateUserNullAndPasswordNotNull() {
    HiveConfig config = HiveConfig.builder(VALID_CONFIG)
      .setUser(null)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(HiveConfig.USER, HiveConfig.PASSWORD));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateUserEmptyAndPasswordNotNull() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HiveConfig config = HiveConfig.builder(VALID_CONFIG)
      .setUser("")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(HiveConfig.USER, HiveConfig.PASSWORD));

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateUserNotNullAndPasswordNull() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HiveConfig config = HiveConfig.builder(VALID_CONFIG)
      .setPassword(null)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(HiveConfig.USER, HiveConfig.PASSWORD));

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateUserNotNullAndPasswordEmpty() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HiveConfig config = HiveConfig.builder(VALID_CONFIG)
      .setPassword("")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(HiveConfig.USER, HiveConfig.PASSWORD));

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateIncorrectStatement() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HiveConfig config = HiveConfig.builder(VALID_CONFIG)
      .setStatement("LOAD DATA LOCAL INPATH 'sample.txt' INTO TABLE test2")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(HiveConfig.STATEMENT));

    config.validateImportStatement(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }
}
