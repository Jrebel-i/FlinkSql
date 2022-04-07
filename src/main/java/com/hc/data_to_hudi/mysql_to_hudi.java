package com.hc.data_to_hudi;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hc.data_to_hudi.until.CustomDeserializationSchemaWithMysql;
import com.hc.data_to_hudi.until.Dao;
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


public class mysql_to_hudi {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("test1124-cluster.ciddqahckccz.us-east-2.rds.amazonaws.com")
                .port(3306)
                .databaseList("ods_sandbox") // set captured database
                .tableList("ods_sandbox.aa","ods_sandbox.bb") // set captured database.table
//                .tableList("ods_sandbox\\..*") // set captured database.table
//                .tableList("ods_sandbox.*\\..*") // set captured database.table
                .username("sandbox")
                .password("sbshenqi:64")
                .includeSchemaChanges(true)
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON Strin
//                .deserializer(new CustomDeserializationSchema())  //自定义反序列化器
                .deserializer(new CustomDeserializationSchemaWithMysql()) // converts SourceRecord to String
                .build();
        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,8088);
        conf.setString("taskmanager.memory.managed.size","4g");
        //设置从checkpoint中恢复
//        conf.setString("execution.savepoint.path", "file:///F:\\BigData_Learning\\CheckPoint\\flink-studay\\9f27b9328c79617a81d9031d55669f68\\chk-50");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //注意：增量同步需要开启CK
        // ck 设置 设置状态后端为RocksDb
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.enableCheckpointing(600000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                "file:///F:\\BigData_Learning\\CheckPoint\\flink-studay", true);
        rocksDBStateBackend.setNumberOfTransferThreads(3);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(rocksDBStateBackend);

//        env
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                .setParallelism(4)
//                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
//        env.execute("Print MySQL Snapshot + Binlog");

//        Schema schema = Schema.newBuilder()
//                .column("id", DataTypes.BIGINT())
//                .column("name", DataTypes.STRING())
//                .column("tbName", DataTypes.STRING())
//                .column("dbName", DataTypes.STRING()).build();

        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        DataStreamSource<String> mySQL_source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        DataStream<Dao> mapStream = mySQL_source.map(new RichMapFunction<String, Dao>() {
            @Override
            public Dao map(String s) throws Exception {
                JSONObject object = JSON.parseObject(s);
                Dao dao = new Dao(
                        object.getString("dbName"),
                        object.getString("tbName"),
                        object.getString("data")
                );
                return dao;
            }
        });
        Table table = tenv.fromDataStream(mapStream);
        tenv.executeSql("show tables").print();
        tenv.createTemporaryView("mysql_source",table);
//        tenv.executeSql("select * \n"+
//                "from mysql_source").print();

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
//                "  'compaction.tasks'= '1',\n" +
//                "  'write.rate.limit'= '2000', -- 限速\n" +
                "  'table.type'= 'MERGE_ON_READ' -- 默认COPY_ON_WRITE,可选MERGE_ON_READ\n" +
//                "  'read.streaming.enabled'= 'true', -- 开启流读\n" +
//                "  'read.streaming.check-interval'= '3' -- 检查间隔，默认60s\n" +
//                hive sync
//                " 'hive_sync.enable'='true',           -- required，开启hive同步功能\n"+
//                " 'hive_sync.table'='mysql_cdc_stream',              -- required, hive 新建的表名\n"+
//                " 'hive_sync.db'='default',             -- required, hive 新建的数据库名\n"+
//                " 'hive_sync.mode' = 'hms',            -- required, 将hive sync mode设置为hms, 默认jdbc\n"+
//                " 'hive_sync.metastore.uris' = 'thrift://ec2-18-117-93-90.us-east-2.compute.amazonaws.com:9083' -- required, metastore的端口\n"+
                ")");
        tenv.executeSql("insert into hudi_users " +
                " select * from mysql_source");
////        tenv.executeSql("select * from hudi_users").print();

    }
}
