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
csv.splitter=,
csv.suffix=csv
```

* `csv.base` sets the base directory. The schema name is the second level directory, and table name is the file name.

* `csv.splitter` sets the delimiter of the CSV file. The default value is `,`.

* `csv.suffix` sets the suffix of the CSV file. The default value is `csv`.

* The first line of the CSV file must be the header.

The connector also supports reading files from a SFTP server.

```
connector.name=csv
csv.protocol=sftp
csv.base=/path/to/dir
csv.splitter=,
csv.suffix=csv
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