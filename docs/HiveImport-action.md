# HiveImport Action


Description
-----------
Imports data from hdfs directory/file into a hive table. Hive Import Action imports data from HDFS by executing provided Hive [Load Statement](https://cwiki.apache.org/confluence/display/Hive/GettingStarted). 
Local file storage is not allowed because a pipeline can run on any machine. If `LOCAL` file storage option is provided,
pipeline deployment fails at publish time. Hive Import only accepts hive `LOAD` statements. If any other hive query is provided,
pipeline publish will fail. Hive import works with Hive 1.2.1.


Use Case
--------
Hive Import Action executes a hive load statement which loads data from HDFS file/directory location into a hive table.


Properties
----------

**user:** User identity for connecting to the specified hive database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication.

**connectionString:** JDBC connection string including database name. Please use auth=delegationToken, 
CDAP platform will provide appropriate delegation token while running the pipeline. 

**statement:** Load command to load files data into a hive table. `LOCAL` option in `LOAD` command is not available.


Example
-------
This example connects to a hive database using the specified 'connectionString', which means
it will connect to the 'mydb' database of a hive instance running on 'localhost' and runs the 
load query. This plugin will read all the files from HDFS path /tmp/hive and load data
to table testTable.

    {
        "name": "HiveExportAction",
        "plugin": {
            "name": "HiveExportAction",
            "type": "action",
            "properties": {
                "user": "username",
                "password": "password",
                "connectionString": "jdbc:hive2://localhost:10000/mydb;auth=delegationToken",
                "statement": "LOAD DATA INPATH '/tmp/hive' INTO TABLE testTable"
            }
        }
    }
