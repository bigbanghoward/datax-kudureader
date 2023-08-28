# 阿里云DataX-KuduReader插件文档

## 1.快速介绍
KuduReader插件实现了从kudu数据源中读取数据，通过调用kudu-client的api连接至KuduMaster,通过调用KuduScanner访问Kudu数据。
## 2.食用方法

### 2.1环境依赖

    Jdk:1.8+;
    Datax:0.0.1-SNAPSHOT;
    Kudu:1.10.0;
    Maven:3.4+;

### 2.1代码Clone

    git clone https://github.com/bigbanghoward/datax-kudureader.git your_dir

### 2.2插件编译

    mvn clean package -DskipTests assembly:assembly

### 2.2插件部署

    cp ./output/datax/plugin/reader/kudureader/* ${DATAX_HOME}/plugin/reader

## 3.配置说明

* 配置一个从Kudu读取数据到本地的作业:

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "kudureader",
          "parameter": {
            "columns": [
              "*"
            ],
            "connection": {
              "kuduMaster": "yourKuduMaster",
              "table": "kuduTableName"
            },
            "outputExtendParam": {
              "扩展参数key": "扩展参数value"
            },
            "where": {
              "column": "p_date",
              "compare": "=",
              "value": "2023-06-23"
            }
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "path": "/tmp",
            "fileName": "kudu_table_data",
            "print": true,
            "encoding": "UTF-8"
          }
        }
      }
    ]
  }
}
```

## 4.参数说明

* columns
    * 描述:要查询的字段列表,list结构,支持'*',可以查询所有字段;
    * 必选:是;
    * 默认值:无;
* kuduMaster
    * 描述:kudu的Master节点地址;
    * 必选:是;
    * 默认值:无;
* table
    * 描述:kudu的数据表名称,支持Impala引擎管理的表,格式为db_name.table_name;
    * 必选:是;
    * 默认值:无;
* where
    * 描述:支持where条件过滤数据，目前只支持单个查询条件，有需求的可以自己修改源码中的[KuduReader.java](..%2Fsrc%2Fmain%2Fjava%2Fcom%2Falibaba%2Fdatax%2Fplugin%2Freader%2Fkudureader%2FKuduReader.java)的handleWhere方法;
    * 必选:否;
    * 默认值:无;
* outputExtendParam
    * 描述:如果想在kudureader的结果输出时附加额外的字段，可以在这里定义，Map结构;
    * 必选:否;
    * 默认值:无;
## 5.测试报表
    测试环境:
        4C16G的测试机进行测试
    测试结果:
	任务启动时刻                    :                   *
	任务结束时刻                    :                   *
	任务总计耗时                    :               1560s
	任务平均流量                    :           14.71MB/s
	记录写入速度                    :          23926rec/s
	读出记录总数                    :            37325658
	读写失败总数                    :                   0
## 6.补充说明
    1.该KuduReader插件的输出是StringColumn类型，内容为json字符串;可以根据自己需要封装Record输出格式;
    