package com.hc.lookup_join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;

public class testRedis {
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
        //维表 redis
        tenv.executeSql("CREATE TABLE user_profile (\n" +
                "    user_id STRING,\n" +
                "    age STRING,\n" +
                "    sex STRING,\n" +
                "   PRIMARY KEY (`user_id`) NOT ENFORCED\n" +
                "    ) WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'host' = '127.0.0.1',\n" +
                "  'mode' = 'string',\n" +
//                "  'mode' = 'hash',\n" +
                "  'port' = '6379'\n" +
                ")");
        tenv.executeSql("insert into user_profile values('xx','0-10','man')");
//        tenv.executeSql("select * from  user_profile ");
//        tenv.executeSql("insert into user_profile values('xx','0-10')");
    }
}
