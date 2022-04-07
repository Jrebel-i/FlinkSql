package com.github.knaufk.flink.faker;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class readFaker_ScanTableSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        //https://github.com/knaufk/flink-faker/
        //ScanTableSource 数据源表
        tEnv.executeSql("CREATE TEMPORARY TABLE heros (\n" +
                "  `name` STRING,\n" +
                "  `power` STRING, \n" +
                "  `age` INT\n" +
                ") WITH (\n" +
                "  'connector' = 'faker', \n" +
                "  'fields.name.expression' = '#{superhero.name}',\n" +
                "  'fields.power.expression' = '#{superhero.power}',\n" +
                "  'fields.power.null-rate' = '0.05',\n" +
                "  'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'\n" +
                ")");

        Table t = tEnv.sqlQuery("SELECT * FROM heros");

        tEnv.toAppendStream(t, Row.class).print();

        env.execute();
    }
}
