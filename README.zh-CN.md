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
csv.protocol=file
csv.base=/path/to/dir
csv.table-description-dir=/path/to/dir
```

`csv.base` 为根目录，schema 名称对应下层目录，table 名称对应 CSV 文件名。

`csv.table-description-dir` 目录存放 JSON 格式的表结构描述文件.

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

| 字段         | 描述                                                            |
|------------|---------------------------------------------------------------|
| schemaName | 库名（路径）                                                        |
| tableName  | 表名（文件名）                                                       |
| wildcard   | 若该选项为 true，tableName 将作为通配符，匹配名称相符、字段相同的所有文件                  |
| delimiter  | CSV 分隔符（默认为 ","）                                              |
| hasHeader  | 若该选项为 true，第一行为字段名（不使用，自动跳过）                                  |
| columns    | 字段描述</br>"type" 可选（"BIGINT"，"BOOLEAN"，"DOUBLE"，默认值 "VARCHAR"） |

也可以从 SFTP 服务器读取文件：

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

## 已知问题

Presto 不支持大写表名（见 [这个 issue](https://github.com/prestodb/presto/issues/2863)），该连接器也不支持文件名中包含大写字母的文件。

## TODO

- [ ] 支持 HDFS、HTTP 等协议。
