package com.hc.formats;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * nc -lk 9999
 */
public class ProtobufFormatTest {

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String sourceTableSql = "CREATE TABLE protobuf_source ("
                + "  name STRING\n"
                + "  , names ARRAY<STRING>\n"
                + "  , si_map MAP<STRING, INT>\n"
                + ")\n"
                + "WITH (\n"
                + "  'connector' = 'socket',\n"
                + "  'hostname' = 'localhost',\n"
                + "  'port' = '9999',\n"
                + "  'format' = 'protobuf',\n"
                + "  'protobuf.class-name' = 'com.hc.formats.protobuf.Test'\n"
                + ")";

        String sinkTableSql = "CREATE TABLE print_sink (\n"
                + "  name STRING\n"
                + "  , names ARRAY<STRING>\n"
                + "  , si_map MAP<STRING, INT>\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

        String selectSql = "INSERT INTO print_sink\n"
                + "SELECT *\n"
                + "FROM protobuf_source\n";

        tEnv.executeSql(sourceTableSql);
        tEnv.executeSql(sinkTableSql);
        tEnv.executeSql(selectSql);

//        env.execute();
    }


}
