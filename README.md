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

Presto does not support case-sensitive identifiers (see [this issue](https://github.com/prestodb/presto/issues/2863)). The connector cannot recognize the file that has uppercase letters in its path either.

## TODO

- [ ] Support HDFS, HTTP Server, etc.

------

Presto CSV connector，支持查询本地或 SFTP 服务器上的 CSV 文件。

## 编译

从 [GitHub](https://github.com/prestodb/presto/) 获取 Presto 源码，将本项目复制进去，在根目录的 `pom.xml` 中添加模块：

```xml
<module>presto-csv</module>
```

## 配置

创建配置文件 `etc/catalog/csv.properties`。读取本地文件的配置如下：

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

* `csv.base` 为根目录，schema 对应二级目录，table 对应二级目录下的文件名。

* `csv.splitter` 可以指定分隔符，默认为 `,`。

* `csv.suffix` 可以指定文件后缀，默认为 `csv`。

*  文件第一行必须是字段名。

也可以从 SFTP 服务器读取文件：

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

## 已知问题

Presto 不支持大写表名（见 [这个 issue](https://github.com/prestodb/presto/issues/2863)），该连接器也不支持读取路径中包含大写字母的文件。

## TODO

- [ ] 支持 HDFS、HTTP 等协议。
