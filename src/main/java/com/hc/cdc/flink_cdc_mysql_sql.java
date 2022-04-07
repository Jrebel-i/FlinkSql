package com.hc.cdc;

import com.utils.JsonMapper;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.types.Row;

import static org.apache.flink.configuration.MetricOptions.LATENCY_INTERVAL;

public class flink_cdc_mysql_sql {
    public static void main(String[] args) throws Exception {

        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,18888);
        //全链路延迟
//        conf.setLong(LATENCY_INTERVAL,30000);
//       //开启普罗米修斯
//        conf.setString("metrics.reporter.promgateway.class","org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter");
//        conf.setString("metrics.reporter.promgateway.host","localhost");
//        conf.setString("metrics.reporter.promgateway.port","9091");
//        conf.setString("metrics.reporter.promgateway.jobName","flink-metrics-ppg");
//        conf.setString("metrics.reporter.promgateway.randomJobNameSuffix","true");
//        conf.setString("metrics.reporter.promgateway.deleteOnShutdown","false");
//        conf.setString("metrics.reporter.promgateway.interval","30 SECONDS");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //设置从checkpoint中恢复
//        conf.setString("execution.savepoint.path", "file:///Users/flink/checkpoints/ce2e1969c5088bf27daf35d4907659fd/chk-5");

        // ck 设置
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.enableCheckpointing(3 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置状态后端为RocksDb
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                "file:///F:\\BigData_Learning\\CheckPoint\\flink-studay", true);
        rocksDBStateBackend.setNumberOfTransferThreads(3);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(rocksDBStateBackend);

        // 配置表在jdbc, 使用flink-sql-cdc来完成
        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        Configuration configuration = tenv.getConfig().getConfiguration();
        configuration.setString("table.local-time-zone","Asia/Shanghai");
        tenv
        .executeSql("CREATE TABLE `table_process`( " +
                "    db_name STRING METADATA FROM 'database_name' VIRTUAL,\n" +
                "    table_name STRING METADATA  FROM 'table_name' VIRTUAL,\n" +
                "    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL," +
                "   `id`  bigint, " +
                "   `name`  varchar(100), " +
                "    PRIMARY KEY(id) NOT ENFORCED "+
                ")with(" +
                "   'connector' = 'mysql-cdc', " +
                "   'hostname' = 'test1124-cluster.ciddqahckccz.us-east-2.rds.amazonaws.com', " +
                "   'port' = '3306', " +
                "   'username' = 'sandbox', " +
                "   'password' = 'sbshenqi:64', " +
                "   'database-name' = 'ods_sandbox', " +
                "   'table-name' = 'aa'," +  //.*
                "   'debezium.snapshot.mode' = 'initial' " +
                // 读取mysql的全量,增量以及更新
                ")");
        //获取sql explain执行计划
        System.out.println(tenv.explainSql("select * from table_process"));

//        System.out.println("============获取DAG================");
//        System.out.println(((TableEnvironmentInternal)env).getJsonPlan("select * from table_process"));
        //calcite解析sql
//        SqlParser sqlParser = SqlParser.create("select * from table_process",SqlParser.Config.DEFAULT);
//        SqlNode sqlNode = sqlParser.parseStmt();
//        System.out.println(JsonMapper.toJsonString(sqlNode));
        tenv.executeSql("select * from table_process").print();

//        DataStream<Tuple2<Boolean, Row>> toRetractStream = tenv.toRetractStream(table, Row.class);
//        toRetractStream.print("toRetractStream");


//        DataStream<Row> toChangelogStream = tenv.toChangelogStream(table);
//        toChangelogStream.print("toChangelogStream");

//        env.execute();
    }
}
