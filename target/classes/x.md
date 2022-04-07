set a "{\"score\":3,\"name\":\"namehhh\",\"name1\":\"namehhh222\"}"
set b "{\"score\":3,\"name\":\"namehhh\",\"name1\":\"namehhh222\"}"

set a "{\"age\":\"12-18\",\"sex\":\"男\"}"
set b "{\"age\":\"18-24\",\"sex\":\"女\"}"
set c "{\"age\":\"18-24\",\"sex\":\"男\"}"

set a "{\"age\":\"12-18\",\"sex\":\"man\"}"
set b "{\"age\":\"18-24\",\"sex\":\"woman\"}"
set c "{\"age\":\"18-24\",\"sex\":\"man\"}"

select 1
HSET aa age "12-18"
HSET aa sex "man"
HSET bb age "12-18"
HSET bb sex "woman"
HSET cc age "18-24"
HSET cc sex "man"

select 2
set aa "age=12-18,sex=man"
set bb "age=12-18,sex=woman"
set cc "age=18-24,sex=man"



引入依赖
//protoc.exe -I=. --java_out=. test.proto
//https://www.jianshu.com/p/bb3ac7e5834e
//软件版本需要和依赖版本一致
<dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>3.19.1</version>
</dependency>

sql==>sqlNode  解析【借助calcite的org.apache.calcite.sql.parser.SqlParser#parseStmt生成的】
在这里用到了org.apache.flink.sql.parser.impl.FlinkSqlParserImpl，
在flink源码中是没有这个类的，但是在jar包中存在（借助calcite的maven 插件生成的）
https://mp.weixin.qq.com/s/SxRKp368mYSKVmuduPoXFg#:~:text=5.2.FlinkSqlParserImpl%20%E7%9A%84%E7%94%9F%E6%88%90
在flink源码中的codegen目录存放了生成parser的所有文件，主要parser逻辑定义在parserlmpls.ftl中
1，使用 maven-dependency-plugin 将 calcite 解压到 flink 项目 build 目录下
2，使用 maven-resources-plugin 将 Parser.jj 代码生成
3，javacc 生成 parser


sqlNode==>sqlNode 校验【借助org.apache.calcite.sql.validate.SqlValidatorImpl#validate生成的】
 AST （抽象语法树）

sqlNode==>RelNode 语义分析【借助org.apache.calcite.sql2rel.SqlToRelConverter#convertQuery生成的】
（生成相应的关系代数层面的逻辑（这里一般都叫做逻辑计划：Logical Plan））就是转换为：关系（代数）表达式
逻辑规划生成一颗操作树(例如ModifyOperation, QueryOperation)
并在方法中打印：
== Abstract Syntax Tree ==
Plan after converting SqlNode to RelNode
LogicalProject(db_name=[CAST($2):VARCHAR(2147483647) CHARACTER SET "UTF-16LE"], table_name=[CAST($4):VARCHAR(2147483647) CHARACTER SET "UTF-16LE"], operation_ts=[CAST($3):TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)], id=[$0], name=[$1])
+- LogicalTableScan(table=[[default_catalog, default_database, table_process]])

RelNode==>RelNode 优化阶段
optimize优化的时间将RelNode转化为optimizedRelNodes
再进行translateToExecNodeGraph将其转化为为ExecGraph
最后经过translateToPlan将ExecGraph转化为Transformation
== Optimized Physical Plan ==
optimize result:
 Sink(table=[default_catalog.default_database.Unregistered_Collect_Sink_1], fields=[db_name, table_name, operation_ts, id, name])
+- Calc(select=[CAST(database_name) AS db_name, CAST(table_name) AS table_name, CAST(op_ts) AS operation_ts, id, name])
   +- TableSourceScan(table=[[default_catalog, default_database, table_process]], fields=[id, name, database_name, op_ts, table_name])

== Optimized Execution Plan ==
Calc(select=[CAST(database_name) AS db_name, CAST(table_name) AS table_name, CAST(op_ts) AS operation_ts, id, name])
+- DropUpdateBefore
   +- TableSourceScan(table=[[default_catalog, default_database, table_process]], fields=[id, name, database_name, op_ts, table_name])

0）通过translateToRel将Operation转为RelNode
1）在Table/SQL 编写完成后，通过Calcite 中的parse、validate、rel阶段，以及Blink额外添加的convert阶段,将其先转为Operation；
rel会将sqlNode==>RelNode，之后借助Blink Planner将RelNode转化为Operation中的calciteTree[LogicalPlan]
2）通过Blink Planner 的translateToRel、optimize、translateToExecNodeGraph和translateToPlan四个阶段，将Operation转换成DataStream API的 Transformation；
3）再经过StreamJraph -> JobGraph -> ExecutionGraph等一系列流程，SQL最终被提交到集群。

![Blink Planner与Calcite对接流程图](img\Blink Planner与Calcite对接流程图.png)

Flink优化规则：FlinkStreamRuleSets