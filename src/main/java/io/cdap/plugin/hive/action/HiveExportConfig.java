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
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.hive.action.common.HiveConfig;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Hive Export config
 */
public class HiveExportConfig extends HiveConfig {
  public static final String DELIMITER = "delimiter";
  public static final String PATH = "path";
  public static final String OVERWRITE = "overwrite";

  @Name(DELIMITER)
  @Description("Delimiter in the exported file. Values in each column is separated by this delimiter while writing to" +
    " output file")
  @Nullable
  @Macro
  private String delimiter;

  @Name(PATH)
  @Description("HDFS directory path where exported data will be written")
  @Macro
  private String path;

  @Name(OVERWRITE)
  @Description("If HDFS directory already exists should it be overwritten?")
  @Macro
  @Nullable
  private String overwrite;

  public HiveExportConfig(String connectionString, String user, String password, String statement,
                          String delimiter, String path, String overwrite) {
    super(connectionString, user, password, statement);
    this.delimiter = delimiter;
    this.path = path;
    this.overwrite = overwrite;
  }

  private HiveExportConfig(Builder builder) {
    super(builder.connectionString, builder.user, builder.password, builder.statement);
    delimiter = builder.delimiter;
    path = builder.path;
    overwrite = builder.overwrite;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(HiveExportConfig copy) {
    return builder()
      .setConnectionString(copy.getConnectionString())
      .setUser(copy.getUser())
      .setPassword(copy.getPassword())
      .setStatement(copy.getStatement())
      .setDelimiter(copy.delimiter)
      .setPath(copy.path)
      .setOverwrite(copy.overwrite);
  }

  @Nullable
  public String getDelimiter() {
    return delimiter;
  }

  public String getPath() {
    return path;
  }

  @Nullable
  public String getOverwrite() {
    return overwrite;
  }

  @Override
  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);

    if (!containsMacro(STATEMENT)) {
      // validate hive command. For export we only accept Select statements
      SqlParser parser = SqlParser.create(getStatement());
      try {
        SqlNode sqlNode = parser.parseQuery();
        if (!(sqlNode instanceof SqlSelect)) {
          failureCollector.addFailure("Hive Export only uses Select statements.",
                                      "Provide valid hive select statement.")
            .withConfigProperty(STATEMENT);
        }
      } catch (SqlParseException e) {
        failureCollector.addFailure("Error while parsing select statement.", null)
          .withStacktrace(e.getStackTrace())
          .withConfigProperty(STATEMENT);
      }
    }

    if (!containsMacro(OVERWRITE) && !containsMacro(PATH)) {
      // validate if the directory already exists
      if (overwrite.equalsIgnoreCase("no")) {
        Configuration configuration = new Configuration();
        try {
          FileSystem fs = FileSystem.get(configuration);
          if(fs.exists(new Path(path))) {
            failureCollector.addFailure(String.format("The path '%s' already exists.", path), null)
              .withConfigProperty(OVERWRITE)
              .withConfigProperty(PATH);
          }
        } catch (IOException e) {
          failureCollector.addFailure("Exception occurred while doing directory check",
                                      "Provide correct directory path.")
            .withStacktrace(e.getStackTrace())
            .withConfigProperty(PATH);
        }
      }
    }
  }

  public static class Builder extends HiveConfig.Builder<Builder> {
    private String delimiter;
    private String path;
    private String overwrite;
    private String connectionString;
    private String user;
    private String password;
    private String statement;

    private Builder() {
    }

    public Builder setDelimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setOverwrite(String overwrite) {
      this.overwrite = overwrite;
      return this;
    }

    public Builder setConnectionString(String connectionString) {
      this.connectionString = connectionString;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setStatement(String statement) {
      this.statement = statement;
      return this;
    }

    public HiveExportConfig build() {
      return new HiveExportConfig(this);
    }
  }
}
