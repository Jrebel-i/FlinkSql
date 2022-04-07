package com.hc.lookup_join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;


public class lookupRedisWithMySource_2_yang {
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
        tenv.getConfig().getConfiguration().setBoolean("is.dim.batch.mode",false);
        DataStream<Row> rowDataStreamSource = env.addSource(new UserDefinedSource());

        Table table = tenv.fromDataStream(rowDataStreamSource,
                $("log_id"),
                $("user_id"),
                $("proctime").proctime());

        tenv.createTemporaryView("show_log", table);

        //维表 redis  存储为Json格式  以string存储
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

        tenv.executeSql(" SELECT \n" +
                "    s.log_id as log_id\n" +
                "    , cast(CURRENT_TIMESTAMP as timestamp(3)) as `timestamp`\n" +
                "    , s.user_id as user_id\n" +
                "    , s.proctime as proctime\n" +
                "    , u.sex as sex\n" +
                "    , u.age as age\n" +
                " FROM show_log AS s " +
                " LEFT JOIN user_profile for SYSTEM_TIME AS OF s.proctime AS u" +
                " on s.user_id=u.user_id").print();

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
