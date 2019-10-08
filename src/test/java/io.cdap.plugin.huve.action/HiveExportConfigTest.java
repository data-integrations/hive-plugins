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
import io.cdap.plugin.hive.action.HiveExportConfig;
import io.cdap.plugin.hive.action.common.HiveConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class HiveExportConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final HiveExportConfig VALID_CONFIG = new HiveExportConfig(
    "jdbc:hive2://localhost:10000/mydb;auth=delegationToken",
    "user",
    "password",
    "SELECT * FROM employee e JOIN salary s ON (e.id = s.id)",
    ",",
    "/path/to/export/directory",
    "yes"
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateNotSelectStatement() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HiveExportConfig config = HiveExportConfig.builder(VALID_CONFIG)
      .setStatement("UPDATE tablename SET \"column\" = 'value'")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(HiveConfig.STATEMENT));

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateIncorrectStatement() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HiveExportConfig config = HiveExportConfig.builder(VALID_CONFIG)
      .setStatement("ALTER tablename SET \"column\" = 'value'")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(HiveConfig.STATEMENT));

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }

}
