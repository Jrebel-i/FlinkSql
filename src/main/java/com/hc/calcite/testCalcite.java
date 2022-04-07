package com.hc.calcite;

import com.utils.JsonMapper;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class testCalcite {
    public static void main(String[] args) throws Exception {

        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        String sql = " INSERT INTO tmp.print_joined_result " +
                " SELECT FROM_UNIXTIME(a.ts / 1000, 'yyyy-MM-dd HH:mm:ss') AS tss, a.userId, a.eventType, a.siteId, b.site_name AS siteName " +
                " FROM rtdw_ods.kafka_analytics_access_log_app /*+ OPTIONS('scan.startup.mode'='latest-offset','properties.group.id'='DiveIntoBlinkExp') */ a " +
                " LEFT JOIN rtdw_dim.mysql_site_war_zone_mapping_relation FOR SYSTEM_TIME AS OF a.procTime AS b ON CAST(a.siteId AS INT) = b.main_site_id " +
                " WHERE a.userId > 7 ";
        tenv.executeSql(sql);
    }
}
