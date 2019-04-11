/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.plugin.hive.action;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.hive.action.common.HiveCommandExecutor;
import io.cdap.plugin.hive.action.common.HiveConfig;

/**
 * Imports data from hdfs directory/file into a hive table.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("HiveImport")
@Description("Hive import plugin")
public class HiveImport extends Action {
  private final HiveConfig config;

  public HiveImport(HiveConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    validateImportConfig();
  }

  private void validateImportConfig() {
    if (!config.containsMacro("statement")) {
      // Load command should not allow local storage
      if (config.statement.length() >= 15 && config.statement.substring(10, 15).equalsIgnoreCase("LOCAL")) {
        throw new IllegalArgumentException("Hive Import does not allow local file storage." +
                                             "Please import data to HDFS location.");
      }
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    validateImportConfig();
    HiveCommandExecutor executor = new HiveCommandExecutor(config.connectionString, config.user, config.password);
    executor.execute(config.statement);
    executor.cleanup();
  }
}
