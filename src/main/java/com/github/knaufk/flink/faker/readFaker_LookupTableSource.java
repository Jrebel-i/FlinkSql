package com.github.knaufk.flink.faker;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class readFaker_LookupTableSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        //https://github.com/knaufk/flink-faker/

        //LookupTableSource 数据维表
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE location_updates (\n" +
                        "  `character_id` INT,\n" +
                        "  `location` STRING,\n" +
                        "  `proctime` AS PROCTIME()\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  'connector' = 'faker', \n" +
                        "  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',\n" +
                        "  'fields.location.expression' = '#{harry_potter.location}'\n" +
                        ")"
        );
        tEnv.executeSql("CREATE TEMPORARY TABLE characters (\n" +
                "  `character_id` INT,\n" +
                "  `name` STRING\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'faker', \n" +
                "  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',\n" +
                "  'fields.name.expression' = '#{harry_potter.characters}'\n" +
                ")");

        Table t = tEnv.sqlQuery("SELECT \n" +
                "  c.character_id,\n" +
                "  l.location,\n" +
                "  c.name\n" +
                "FROM location_updates AS l\n" +
                "JOIN characters FOR SYSTEM_TIME AS OF proctime AS c\n" +
                "ON l.character_id = c.character_id");

        tEnv.toAppendStream(t, Row.class).print();

        env.execute();
    }
}
