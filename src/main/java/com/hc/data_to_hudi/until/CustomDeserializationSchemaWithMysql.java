package com.hc.data_to_hudi.until;


import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


public class CustomDeserializationSchemaWithMysql implements DebeziumDeserializationSchema<String>{
    //自定义数据解析器
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink.z_user_info
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];

        //获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        //获取值信息并转换为Struct类型
        Struct value = (Struct) sourceRecord.value();

        //获取变化后的数据
        Struct after = value.getStruct("after");

        //创建JSON对象用于存储数据信息
        JSONObject data = new JSONObject();
        //创建JSON对象用于封装最终返回值数据信息
        JSONObject result = new JSONObject();
        for (Field field : after.schema().fields()) {
            Object o = after.get(field);
            data.put(field.name(), o);
        }

//        result.put("operation", operation.toString().toLowerCase());
        result.put("data", data);
        result.put("dbName", db);
        result.put("tbName", tableName);

        //发送数据至下游
        collector.collect(result.toJSONString());
    }

    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

}
