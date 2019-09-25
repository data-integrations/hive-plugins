/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.hive.action.common.HiveCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive Export runs a select query against a hive table and stores results under an hdfs directory.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("HiveExport")
@Description("Hive export plugin")
public class HiveExport extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(HiveExport.class);
  private final HiveExportConfig config;

  public HiveExport(HiveExportConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    // Create Insert command for hive
    String command = "INSERT OVERWRITE DIRECTORY '" + config.getPath() +
      "' row format delimited  FIELDS TERMINATED BY '" + config.getDelimiter() + "' " + config.getStatement();

    LOG.debug("Hive command being executed: {}", command);

    HiveCommandExecutor executor = new HiveCommandExecutor(config.getConnectionString(), config.getUser(),
                                                           config.getPassword());
    executor.execute(command);
    executor.cleanup();
  }
}
