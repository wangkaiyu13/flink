package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyStringDeserializationSchema implements DebeziumDeserializationSchema<String> {
    //{
    // "database":"",
    // "tableName":"",
    // "data":{"id":"1001","tm_name","atguigu"....},
    // "before":{"id":"1001","tm_name","atguigu"....},
    // "type":"update",
    // "ts":141564651515
    // }
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //构建结果对象
        JSONObject result = new JSONObject();

        //获取数据库名称&表名称
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        //获取数据
        Struct value = (Struct) sourceRecord.value();

        //After
        Struct after = value.getStruct("after");
        JSONObject data = new JSONObject();
        if (after != null) { //delete数据,则after为null
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();

            for (int i = 0; i < fieldList.size(); i++) {
                Field field = fieldList.get(i);
                Object fieldValue = after.get(field);
                data.put(field.name(), fieldValue);
            }
        }

        //Before
        Struct before = value.getStruct("before");
        JSONObject beforeData = new JSONObject();
        if (before != null) { //delete数据,则after为null
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();

            for (int i = 0; i < fieldList.size(); i++) {
                Field field = fieldList.get(i);
                Object fieldValue = before.get(field);
                beforeData.put(field.name(), fieldValue);
            }
        }

        //获取操作类型 CREATE UPDATE DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        //封装数据
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("data", data);
        result.put("before", beforeData);
        result.put("type", type);
        //result.put("ts", System.currentTimeMillis());

        //输出封装好的数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
