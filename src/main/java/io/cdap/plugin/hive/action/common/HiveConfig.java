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

package io.cdap.plugin.hive.action.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Hive config
 */
public class HiveConfig extends PluginConfig {
  public static final String CONNECTION_STRING = "connectionString";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String STATEMENT = "statement";

  @Name(CONNECTION_STRING)
  @Description("JDBC connection string including database name. Please use auth=delegationToken, " +
    "CDAP platform will provide appropriate delegation token while running the pipeline")
  @Macro
  public String connectionString;

  @Name(USER)
  @Description("User to use to connect to hive metastore database. " +
    "Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String user;

  @Name(PASSWORD)
  @Description("Password to use to connect to hive metastore database. " +
    "Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String password;

  @Name(STATEMENT)
  @Description("Hive command to execute")
  @Macro
  public String statement;

  public HiveConfig(String connectionString, String user, String password, String statement) {
    this.connectionString = connectionString;
    this.user = user;
    this.password = password;
    this.statement = statement;
  }
}
