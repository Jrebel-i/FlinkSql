package com.hc.lookup_join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class lookupRedisWithDataGen {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,8081);
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

        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //流表
        tenv.executeSql("CREATE TABLE show_log (\n" +
                "    log_id BIGINT,\n" +
                "    `timestamp` as cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    user_id STRING,\n" +
                "    proctime AS PROCTIME()\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.user_id.length' = '1',\n" +
                "  'fields.log_id.min' = '1',\n" +
                "  'fields.log_id.max' = '10'\n" +
                ")");

        //维表
        tenv.executeSql("CREATE TABLE user_profile (\n" +
                "    user_id STRING,\n" +
                "    age STRING,\n" +
                "    sex STRING\n" +
                "    ) WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'hostname' = '127.0.0.1',\n" +
                "  'port' = '6379',\n" +
                "  'format' = 'json',\n" +
                "  'lookupRedisWithDataGen.cache.max-rows' = '500',\n" +
                "  'lookupRedisWithDataGen.cache.ttl' = '3600',\n" +
                "  'lookupRedisWithDataGen.max-retries' = '1'\n" +
                ")");
        //输出表
        tenv.executeSql("CREATE TABLE sink_table (\n" +
                "    log_id BIGINT,\n" +
                "    `timestamp` TIMESTAMP(3),\n" +
                "    user_id STRING,\n" +
                "    proctime TIMESTAMP(3),\n" +
                "    age STRING,\n" +
                "    sex STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        tenv.executeSql("insert into sink_table" +
                " SELECT \n" +
                "    s.log_id as log_id\n" +
                "    , s.`timestamp` as `timestamp`\n" +
                "    , s.user_id as user_id\n" +
                "    , s.proctime as proctime\n" +
                "    , u.sex as sex\n" +
                "    , u.age as age\n" +
                " FROM show_log AS s " +
                " LEFT JOIN user_profile for SYSTEM_TIME AS OF s.proctime AS u" +
                " on s.user_id=u.user_id");

    }
}
