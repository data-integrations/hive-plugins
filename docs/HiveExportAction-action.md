# VerticaBulkExportAction


Description
-----------
Bulk exports data in a vertica table into a file.


Use Case
--------
Vertica Bulk Export Action plugin gets executed after successful mapreduce or spark job. It executes select query on a vertica table and writes results in a provided file location in csv format.


Properties
----------

**path:** HDFS File path where exported data will be written.

**delimiter:** Delimiter in the output file. Values in each column is separated by this delimiter while writing to output file.

**user:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication.

**connectionString:** JDBC connection string including database name.

**selectStatement:** Select command to select values from a vertica table. 


Example
-------
This example connects to a vertica database using the specified 'connectionString', which means
it will connect to the 'test' database of a vertica instance running on 'localhost' and run the 
select query. This plugin will read all the columns as String and write to file /tmp/vertica/vertica_export.csv
on HDFS. We can also specify column delimiter which is comman in below properties.

    {
        "name": "VerticaBulkExportAction",
        "plugin": {
            "name": "VerticaBulkExportAction",
            "type": "action",
            "properties": {
                "path": "/tmp/vertica/vertica_export.csv",
                "delimiter": ",",
                "user": "username",
                "password": "password",
                "connectionString": "jdbc:localhost:5433/test",
                "selectStatement": "Select * from testTable"
            }
        }
    }
