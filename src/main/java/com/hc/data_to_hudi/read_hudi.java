package com.hc.data_to_hudi;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class read_hudi {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,8089);
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


        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        tenv.executeSql("CREATE TABLE hudi_users (\n" +
//                "  id BIGINT(20),\n" +
                "  dbName VARCHAR(20),\n" +
                "  tbName VARCHAR(20),\n" +
                "  data  VARCHAR(20)\n" +
                ")\n" +
                "PARTITIONED BY (dbName,tbName)\n" +
                "with(\n" +
                "  'connector'='hudi',\n" +
                "  'path' = 's3a://big-data-warehouse-ods/test/mysql-cdc/',\n" +
                "  'hoodie.datasource.write.recordkey.field'= 'data', -- 主键\n" +
                "  'compaction.tasks'= '1',\n" +
                "  'write.rate.limit'= '2000', -- 限速\n" +
                "  'table.type'= 'MERGE_ON_READ', -- 默认COPY_ON_WRITE,可选MERGE_ON_READ\n" +
                "  'read.streaming.enabled'= 'true', -- 开启流读\n" +
                "  'read.streaming.check-interval'= '3' -- 检查间隔，默认60s\n" +
                ")");
        tenv.executeSql("select * from hudi_users").print();

    }
}
