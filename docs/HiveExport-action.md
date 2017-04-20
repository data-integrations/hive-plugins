HiveExport Action
=================

Description
-----------
Hive Export will take select query as input to run that query on hive table and store results under provided HDFS directory. When the select query is provided to the plugin,
it converts that select query to [INSERT OVERWRITE DIRECTORY](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML) hive statement.
When this query is executed, hive starts a mapreduce job which stores the results to provided directory location. So there can be multiple files in
a given directory location. Hive Export works with hive 1.2.1.

If any query other than a valid SELECT query is provided, Hive Export will fail to publish the pipeline. This is becuase we use [Apache Calcite](https://calcite.apache.org/)
to parse the SELECT query to verify that its not any other SQL Query.

To run the SELECT query, if `Overwrite Output Directory` property is set to `no`, the pipeline publish will fail if the output directory already exists. In that case,
please either remove the directory or allow directory to be overwritten by specifying `Overwrite Output Directory` property to `yes`.

Use Case
--------
Hive Export Action executes a select query on hive table(s) and writes results in a provided directory location in csv format.


Properties
----------

**user:** User identity for connecting to the specified hive database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication.

**connectionString:** JDBC connection string including database name. Please use auth=delegationToken, 
CDAP platform will provide appropriate delegation token while running the pipeline. 

**statement:** Select command to select values from hive table(s).

**path:** HDFS Directory path where exported data will be written. If it does not exist it will get created. 
If it already exists, we can either overwrite it or fail at publish time based on `Overwrite Output Directory` property.

**overwrite:** If yes is selected, if the HDFS `path` exists, it will be overwritten. If no is selected, if the HDFS path exists,
 pipeline deployment will fail while publishing the pipeline.

**delimiter:** Delimiter in the exported file. Values in each column is separated by this delimiter while writing 
to output file. By default, it uses comma.


Example
-------
This example connects to a hive database using the specified 'connectionString', which means
it will connect to the 'mydb' database of a hive instance running on 'localhost' and runs the 
select query as 'INSERT OVERWRITE DIRECTORY' statement. It will use path directory /tmp/hive and delimiter comma
to write data into file(s).

    {
        "name": "HiveExport",
        "plugin": {
            "name": "HiveExport",
            "type": "action",
            "properties": {
                "path": "/tmp/hive",
                "overwrite": "yes",
                "delimiter": ",",
                "user": "username",
                "password": "password",
                "connectionString": "jdbc:hive2://localhost:10000/mydb;auth=delegationToken",
                "statement": "SELECT * FROM employee JOIN salary ON (employee.id = salary.id)"
            }
        }
    }
