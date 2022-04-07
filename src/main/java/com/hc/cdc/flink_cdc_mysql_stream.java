package com.hc.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class flink_cdc_mysql_stream {
    public static void main(String[] args) throws Exception {

        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,8082);
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

        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname("test1124-cluster.ciddqahckccz.us-east-2.rds.amazonaws.com")
                        .port(3306)
                        .databaseList("ods_sandbox")
                        .tableList("ods_sandbox\\..*")
                        .username("sandbox")
                        .password("sbshenqi:64")
//                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema(true))
                        .includeSchemaChanges(true) // 这里配置，输出DDL事件
                        .build();

        DataStreamSource<String> mySQL_source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        SingleOutputStreamOperator<JSONObject> map = mySQL_source.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject strJson = JSON.parseObject(value);
                JSONObject payload = strJson.getJSONObject("payload");
                String db = payload.getJSONObject("source").getString("db");
                String table = payload.getJSONObject("source").getString("table");

                JSONObject before = payload.getJSONObject("before");
                if(before==null){
                    before=new JSONObject();
                    before.put("dbName", db);
                    before.put("tableName", table);
                    payload.put("before", before);
                }else{
                    before.put("dbName", db);
                    before.put("tableName", table);
                    payload.put("before", before);
                }

                JSONObject after = payload.getJSONObject("after");
                if(after==null){
                    after=new JSONObject();
                    after.put("dbName", db);
                    after.put("tableName", table);
                    payload.put("after", after);
                }else {
                    after.put("dbName", db);
                    after.put("tableName", table);
                    payload.put("after", after);
                }
                return payload;
            }
        });

        map.print("读取cdc数据");

        env.execute();
    }
}
