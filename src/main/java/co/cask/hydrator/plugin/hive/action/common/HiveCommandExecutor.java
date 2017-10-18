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

package co.cask.hydrator.plugin.hive.action.common;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Executor which executes provided hive commands usin jdbc
 */
public class HiveCommandExecutor {
  Object driver;
  Connection connection;

  public HiveCommandExecutor(String connectionString, String user, String password) throws Exception {
    driver = Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance();
    DriverManager.registerDriver((Driver) driver);
    connection = DriverManager.getConnection(connectionString, user, password);
  }

  public void execute(String command) throws Exception {
      Statement statement = connection.createStatement();
      statement.execute("SET hive.exec.dynamic.partition = true");
      statement.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
      statement.execute(command);
      statement.close();
  }

  public void cleanup() throws Exception {
    connection.close();
    DriverManager.deregisterDriver((Driver) driver);
  }
}
