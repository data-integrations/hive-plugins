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

package co.cask.hydrator.plugin.hive.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.hydrator.plugin.hive.action.common.HiveCommandExecutor;
import co.cask.hydrator.plugin.hive.action.common.HiveConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a select query after a pipeline run.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("HiveImport")
@Description("Hive import plugin")
public class HiveImport extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(HiveImport.class);
  private final HiveConfig config;

  public HiveImport(HiveConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    //validate hive command. For import we only accept Load statements
    if (!config.statement.toUpperCase().startsWith("LOAD")) {
      throw new IllegalArgumentException("To import data to a hive table, please use Hive LOAD query");
    }

    if (config.statement.length() < 15) {
      throw new IllegalArgumentException("Invalid LOAD query. Please use correct Hive LOAD query");
    }

    // Load command should not allow local storage
    if (config.statement.substring(10, 15).equalsIgnoreCase("LOCAL")) {
      throw new IllegalArgumentException("Hive Import does not allow local file storage." +
                                           "Please import data to HDFS location.");
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    HiveCommandExecutor executor = new HiveCommandExecutor(config.connectionString, config.user, config.password);
    executor.execute(config.statement);
    executor.cleanup();
  }
}
