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

package io.cdap.plugin.hive.action.common;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Hive config
 */
public class HiveConfig extends PluginConfig {
  public static final String CONNECTION_STRING = "connectionString";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String STATEMENT = "statement";

  private static final String CONNECTION_STRING_PREFIX = "jdbc:hive2://";

  @Name(CONNECTION_STRING)
  @Description("JDBC connection string including database name. Please use auth=delegationToken, " +
    "CDAP platform will provide appropriate delegation token while running the pipeline")
  @Macro
  private String connectionString;

  @Name(USER)
  @Description("User to use to connect to hive metastore database. " +
    "Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  private String user;

  @Name(PASSWORD)
  @Description("Password to use to connect to hive metastore database. " +
    "Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  private String password;

  @Name(STATEMENT)
  @Description("Hive command to execute")
  @Macro
  private String statement;

  public HiveConfig(String connectionString, String user, String password, String statement) {
    this.connectionString = connectionString;
    this.user = user;
    this.password = password;
    this.statement = statement;
  }

  private HiveConfig(Builder<?> builder) {
    connectionString = builder.connectionString;
    user = builder.user;
    password = builder.password;
    statement = builder.statement;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(HiveConfig copy) {
    return builder()
      .setConnectionString(copy.connectionString)
      .setUser(copy.user)
      .setPassword(copy.password)
      .setStatement(copy.statement);
  }

  public String getConnectionString() {
    return connectionString;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  public String getStatement() {
    return statement;
  }

  public void validate(FailureCollector failureCollector) {
    if (!containsMacro(CONNECTION_STRING) && !connectionString.startsWith(CONNECTION_STRING_PREFIX)) {
      failureCollector.addFailure(
        "Invalid connection string.",
        "Connection String must comply with format - " +
          "jdbc:hive2://<HiveHost>:<portNumber>/<databaseName>;auth=delegationToken")
        .withConfigProperty(CONNECTION_STRING);
    }

    if (!containsMacro(USER) && !containsMacro(PASSWORD)) {
      if (Strings.isNullOrEmpty(user) && !Strings.isNullOrEmpty(password)) {
        failureCollector.addFailure(
          "Username is not specified.",
          "Ensure both username and password are provided.")
          .withConfigProperty(USER);
      }
      if (!Strings.isNullOrEmpty(user) && Strings.isNullOrEmpty(password)) {
        failureCollector.addFailure(
          "Password is not specified.",
          "Ensure both username and password are provided.")
          .withConfigProperty(PASSWORD);
      }
    }
  }

  public void validateImportStatement(FailureCollector failureCollector) {
    if (!containsMacro(STATEMENT)) {
      // Load command should not allow local storage
      List<String> statementCommandsList = Arrays.stream(statement.split(" ")).filter(value -> !value.isEmpty())
        .collect(Collectors.toList());
      if (statementCommandsList.get(2).equalsIgnoreCase("LOCAL")) {
        failureCollector.addFailure("Hive Import does not allow local file storage.",
                                    "Import data to HDFS location.")
          .withConfigProperty(STATEMENT);
      }
    }
  }

  public static class Builder<T extends Builder<T>> {
    private String connectionString;
    private String user;
    private String password;
    private String statement;

    protected Builder() {
    }

    @SuppressWarnings("unchecked")
    public T setConnectionString(String connectionString) {
      this.connectionString = connectionString;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setUser(String user) {
      this.user = user;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setPassword(String password) {
      this.password = password;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setStatement(String statement) {
      this.statement = statement;
      return (T) this;
    }

    public HiveConfig build() {
      return new HiveConfig(this);
    }
  }
}
