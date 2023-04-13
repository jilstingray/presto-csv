# Presto CSV Connector

Presto CSV 连接器，支持查询本地或 SFTP 服务器上的 CSV 文件。

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

* `csv.base` 为根目录，schema 名称对应下层目录，table 名称对应文件名。

* `csv.splitter` 为 CSV 分隔符，默认为 `,`。

* `csv.suffix` 为 CSV 文件后缀，默认为 `csv`。

* CSV 文件第一行为字段名。

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

Presto 不支持大写表名（见 [这个 issue](https://github.com/prestodb/presto/issues/2863)），该连接器也不支持文件名中包含大写字母的文件。

## TODO

- [ ] 支持 HDFS、HTTP 等协议。
