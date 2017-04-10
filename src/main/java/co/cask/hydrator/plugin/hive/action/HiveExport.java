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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Runs a select query after a pipeline run.
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
    //validate hive command. For export we only accept Select statements
    SqlParser parser = SqlParser.create(config.statement);
    try {
      SqlNode sqlNode = parser.parseQuery();
      if (!(sqlNode instanceof SqlSelect)) {
        throw new IllegalArgumentException("Hive Export only uses Select statements. Please provide valid hive " +
                                             "select statement.");
      }
    } catch (SqlParseException e) {
      throw new IllegalArgumentException("Error while parsing select statemnt. Please provide a valid hive select " +
                                           "statement.");
    }

    // validate if the directory already exists
    if (config.overwrite.equalsIgnoreCase("no")) {
      Configuration configuration = new Configuration();
      try {
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(new Path(config.path))) {
          throw new IllegalArgumentException(String.format("Exception "));
        }
      } catch (IOException e) {
        throw new RuntimeException("Exception occurred while doing directory check", e);
      }

    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    // Create Insert command for hive
    String command = "INSERT OVERWRITE DIRECTORY '" + config.path +
      "' row format delimited  FIELDS TERMINATED BY '" + config.delimiter + "' " + config.statement;

    LOG.debug("Hive command being executed: {}", command);

    HiveCommandExecutor executor = new HiveCommandExecutor(config.connectionString, config.user, config.password);
    executor.execute(command);
    executor.cleanup();
  }
}
