package com.hc.redis;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;


public class RedisSourceLookupTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT,8082);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        DataStream<Row> rowDataStreamSource = env.addSource(new UserDefinedSource());

        Table table = tenv.fromDataStream(rowDataStreamSource,
                $("log_id"),
                $("user_id"),
                $("proctime").proctime());

        tenv.createTemporaryView("show_log", table);


        tenv.executeSql("CREATE TABLE user_profile (\n"
                +"    user_id STRING,\n"
                +"    age STRING,\n"
                +"    sex STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'redis-test',\n"
                + "  'hostname' = '127.0.0.1',\n"
                + "  'port' = '6379',\n"
                + "  'format' = 'json',\n"
                + "  'lookup.cache.max-rows' = '500',\n"
                + "  'lookup.cache.ttl' = '3600',\n"
                + "  'lookup.max-retries' = '1'\n"
                + ")");

        Table t=tenv.sqlQuery(" SELECT \n" +
                "    s.log_id as log_id\n" +
                "    , cast(CURRENT_TIMESTAMP as timestamp(3)) as `timestamp`\n" +
                "    , s.user_id as user_id\n" +
                "    , s.proctime as proctime\n" +
                "    , u.sex as sex\n" +
                "    , u.age as age\n" +
                " FROM show_log AS s " +
                " LEFT JOIN user_profile for SYSTEM_TIME AS OF s.proctime AS u" +
                " on s.user_id=u.user_id");

        tenv.toAppendStream(t, Row.class).print();

        env.execute();
    }


    private static class UserDefinedSource implements SourceFunction<Row>, ResultTypeQueryable<Row> {

        private volatile boolean isCancel;

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {
            while (!this.isCancel) {
                String str="";
                switch (new Random().nextInt(4)){
                    case 0: str="a"; break;
                    case 1: str="b"; break;
                    case 2: str="c"; break;
                    default: str="d";break;
                }

                sourceContext.collect(Row.of(new Random().nextInt(10), str));
                Thread.sleep(100L);
            }
        }
        @Override
        public void cancel() {
            this.isCancel = true;
        }
        @Override
        public TypeInformation<Row> getProducedType() {
            return new RowTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(String.class));
        }
    }

}
