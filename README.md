# Presto CSV Connector

This connector allows Presto to query data stored in CSV files from local or SFTP storage.

## Compile

Download the source code of Presto from [GitHub](https://github.com/prestodb/presto/), copy `presto-csv` into it, and add the following line to `pom.xml` under the root module:

```xml

<module>presto-csv</module>
```

## Configuration

Create a catalog properties file `etc/catalog/csv.properties`. If you want to read files from local drives, use the following configuration:

```
connector.name=csv
csv.protocol=file
csv.base=/path/to/dir
csv.table-description-dir=/path/to/dir
```

`csv.base` sets the root directory. The schema name is the second level directory, and table name is the Excel file name without suffix.

`csv.table-description-dir` contains necessary table description JSON files.

```json
{
  "schemaName": "ip_port_list",
  "tableName": "ip_port_list_1.csv",
  "wildcard": false,
  "delimiter": ",",
  "hasHeader": false,
  "columns": [
    {
      "name": "ip"
    },
    {
      "name": "port",
      "type": "BIGINT"
    }
  ]
}
```

| Field      | Description                                                                                                                            |
|------------|----------------------------------------------------------------------------------------------------------------------------------------|
| schemaName | The schema (folder) name of tables (files).                                                                                            |
| tableName  | The table (file) name.                                                                                                                 |
| wildcard   | If wildcard is true, tableName is a glob pattern.</br>This option supports reading multiple files with similar names and same columns. |
| delimiter  | The delimiter of the file (default ",").                                                                                               |
| hasHeader  | If hasHeader is true, the first line of the file is the header.</br>Whether the file has a header or not, columns must be specified.   |
| columns    | The columns of the table.</br>"type" is optional ("BIGINT", "BOOLEAN", "DOUBLE", default value is "VARCHAR").                          |

The connector also supports reading files from a SFTP server.

```
connector.name=csv
csv.protocol=sftp
csv.base=/path/to/dir
csv.table-description-dir=/path/to/dir
csv.host=xxx.xxx.xxx.xxx
csv.port=xxx
csv.username=xxx
csv.password=xxx
```

## Known issues

Presto does not support uppercase table names (see [this issue](https://github.com/prestodb/presto/issues/2863)). The connector can not recognize the file if its name contains
uppercase letters either.

## TODO

- [ ] Support HDFS, HTTP Server, etc.