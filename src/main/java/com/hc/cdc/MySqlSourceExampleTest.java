package com.hc.cdc;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

public class MySqlSourceExampleTest {

    public static void main(String[] args) throws Exception {
        // 1 通过FlinkCDC构建sourceDatabase
//        DebeziumSourceFunction<String> sourceDatabase = MySqlSource.<String>builder()
//                .hostname("localhost")
//                .port(3306)
//                // 需要监控的database
//                .databaseList("s3_data")
//                .username("root")
//                .password("123456")
//                .tableList("bb")
//                .serverTimeZone("UTC")
//                // 反序列化
//                .deserializer(new StringDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.earliest())
//                .build();
//
//        // 2 创建执行环境
//        Configuration conf=new Configuration();
//        conf.setInteger(RestOptions.PORT,8081);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        env.setParallelism(1);
//        env.enableCheckpointing(10000);
//
//        DataStreamSource<String> dataStreamSource = env.addSource(sourceDatabase);
//        // 3 打印数据
//        dataStreamSource.print();
//        // 4 启动任务
//        env.execute();


//        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
//                .hostname("localhost")
//                .port(3306)
//                // 需要监控的database
//                .databaseList("s3_data")
//                .username("root")
//                .password("123456")
//                .tableList("bb")
//                .serverTimeZone("UTC")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
//                .build();
//        Configuration conf=new Configuration();
//        conf.setInteger(RestOptions.PORT,8081);
//        conf.setString("table.local-time-zone","Asia/Shanghai");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        env.enableCheckpointing(10000);
//
//        env
//                .addSource(sourceFunction)
//                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
//        env.execute();
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("s3_data") // set captured database
                .tableList("s3_data.bb") // set captured database.table
                .username("root")
                .password("123456")
                .includeSchemaChanges(true)
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON Strin
//                .deserializer(new CustomDeserializationSchema())  //自定义反序列化器
                .build();
        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,8081);
        //设置从checkpoint中恢复
//        conf.setString("execution.savepoint.path", "file:///F:\\BigData_Learning\\CheckPoint\\flink-studay\\9f27b9328c79617a81d9031d55669f68\\chk-50");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //注意：增量同步需要开启CK
        // ck 设置 设置状态后端为RocksDb
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                "file:///F:\\BigData_Learning\\CheckPoint\\flink-studay", true);
        rocksDBStateBackend.setNumberOfTransferThreads(3);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(rocksDBStateBackend);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}