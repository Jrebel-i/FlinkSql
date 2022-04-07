package com.hc.data_to_hudi;

import com.alibaba.fastjson.JSONObject;
import com.hc.data_to_hudi.until.CustomDeserializationSchemaWithMysql;
import com.hc.data_to_hudi.until.Dao2;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

public class mysql_to_hudi2 {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("test1124-cluster.ciddqahckccz.us-east-2.rds.amazonaws.com")
                .port(3306)
                .databaseList("ods_sandbox") // set captured database
                .tableList("ods_sandbox.bb","ods_sandbox.aa") // set captured database.table
                .username("sandbox")
                .password("sbshenqi:64")
                .includeSchemaChanges(true)
                .deserializer(new CustomDeserializationSchemaWithMysql()) // converts SourceRecord to String
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON Strin
//                .deserializer(new CustomDeserializationSchema())  //自定义反序列化器
                .build();
        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,18888);
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
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("ods_sandbox_aa","CREATE TABLE ods_sandbox_aa (\n" +
                "  id BIGINT,\n" +
                "  name  VARCHAR(20)\n" +
                ")\n" +
                "with(\n" +
                "  'connector'='hudi',\n" +
                "  'path' = 's3a://big-data-warehouse-ods/test/mysql-cdc2/ods_sandbox/aa',\n" +
                "  'hoodie.datasource.write.recordkey.field'= 'id', -- 主键\n" +
                "  'compaction.tasks'= '1',\n" +
                "  'write.rate.limit'= '2000', -- 限速\n" +
                "  'table.type'= 'MERGE_ON_READ', -- 默认COPY_ON_WRITE,可选MERGE_ON_READ\n" +
                "  'read.streaming.enabled'= 'true', -- 开启流读\n" +
                "  'read.streaming.check-interval'= '3' -- 检查间隔，默认60s\n" +
                ")");
        hashMap.put("ods_sandbox_bb","CREATE TABLE ods_sandbox_bb (\n" +
                "  id BIGINT,\n" +
                "  name  VARCHAR(20)\n" +
                ")\n" +
                "with(\n" +
                "  'connector'='hudi',\n" +
                "  'path' = 's3a://big-data-warehouse-ods/test/mysql-cdc2/ods_sandbox/bb',\n" +
                "  'hoodie.datasource.write.recordkey.field'= 'id', -- 主键\n" +
                "  'compaction.tasks'= '1',\n" +
                "  'write.rate.limit'= '2000', -- 限速\n" +
                "  'table.type'= 'MERGE_ON_READ', -- 默认COPY_ON_WRITE,可选MERGE_ON_READ\n" +
                "  'read.streaming.enabled'= 'true', -- 开启流读\n" +
                "  'read.streaming.check-interval'= '3' -- 检查间隔，默认60s\n" +
                ")");

        for (String key : hashMap.keySet()) {
            System.out.println(hashMap.get(key));
            tenv.executeSql(hashMap.get(key));
        }

        DataStreamSource<String> mySQL_source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        SingleOutputStreamOperator<Dao2> mapStream = mySQL_source.process(new ProcessFunction<String, Dao2>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Dao2> out) throws Exception {
                JSONObject object = JSONObject.parseObject(value);
                if((object.getString("dbName")+"_"+object.getString("tbName")).equalsIgnoreCase("ods_sandbox_aa")){
//                    out.collect(Row.of(object.getLong("id"),object.getString("name")));
                    out.collect(new Dao2(object.getLong("id"),object.getString("name")));
                }else {
//                    ctx.output(new OutputTag<Row>("ods_sandbox_bb"){},Row.of(object.getLong("id"),object.getString("name")));
                    Dao2 dao2 = new Dao2(object.getLong("id"), object.getString("name"));
                    ctx.output(new OutputTag<Dao2>("ods_sandbox_bb"){},dao2);
                }
            }
        });
        Table table = tenv.fromDataStream(mapStream);
        tenv.createTemporaryView("mysql_source_ods_sandbox_aa",table);

        DataStream<Dao2> sideStream = mapStream.getSideOutput(new OutputTag<Dao2>("ods_sandbox_bb") {});
        Table table2 = tenv.fromDataStream(sideStream);
        tenv.createTemporaryView("mysql_source_ods_sandbox_bb",table2);


        tenv.executeSql("insert into ods_sandbox_aa " +
                " select * from mysql_source_ods_sandbox_aa");
//        tenv.executeSql("" +
//                " select * from mysql_source_ods_sandbox_aa").print();

//        tenv.executeSql("insert into ods_sandbox_bb " +
//                " select * from mysql_source_ods_sandbox_bb");
//        tenv.executeSql("" +
//                " select * from mysql_source_ods_sandbox_bb").print();
    }
}
