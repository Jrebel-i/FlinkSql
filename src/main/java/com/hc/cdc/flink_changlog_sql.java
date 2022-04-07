package com.hc.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class flink_changlog_sql {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

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
                .executeSql("CREATE TABLE table_process(\n" +
                        "  id INT NOT NULL,\n" +
                        "  ts BIGINT,\n" +
                        "  name STRING,\n" +
                        "  description STRING,\n" +
                        "  weight DOUBLE\n" +
                        ") WITH (\n" +
                        "  'connector' = 'filesystem',\n" +
                        "  'path' = 'E:\\BigData_Learning\\Own\\FlinkSql\\src\\main\\java\\com\\hc\\cdc\\data\\source.data',\n" +
                        "  'format' = 'debezium-json'\n" +
                        ")");
        tenv.executeSql("select * from table_process").print();
    }
}
