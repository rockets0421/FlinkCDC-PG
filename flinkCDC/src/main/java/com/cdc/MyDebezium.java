package com.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.time.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class MyDebezium implements DebeziumDeserializationSchema<String> {

    // 日期格式转换时区
    private static String serverTimeZone = "Asia/Shanghai";
    // 定义日期时间格式
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 1. 创建一个JSONObject用来存放最终封装好的数据
        JSONObject result = new JSONObject();

        // 2. 解析主键
        Struct  key = (Struct)sourceRecord.key();
        JSONObject keyJs = parseStruct(key);

        // 3. 解析值
        Struct value = (Struct) sourceRecord.value();
        Struct source = value.getStruct("source");

        JSONObject beforeJson = parseStruct(value.getStruct("before"));
        JSONObject afterJson = parseStruct(value.getStruct("after"));

        //将数据封装到JSONObject中
        result.put("db", source.get("db").toString().toLowerCase());
        //result.put("schema", source.get("schema").toString().toUpperCase()); 架构名 看是否需要
        result.put("table", source.get("table").toString().toLowerCase());
        result.put("key", keyJs);
        result.put("op", value.get("op").toString());
        result.put("op_ts", LocalDateTime.ofInstant(Instant.ofEpochMilli(source.getInt64("ts_ms")), ZoneId.of(serverTimeZone)).format(formatter));
        result.put("current_ts", LocalDateTime.ofInstant(Instant.ofEpochMilli(value.getInt64("ts_ms")), ZoneId.of(serverTimeZone)).format(formatter));
        result.put("before", beforeJson);
        result.put("after", afterJson);


        //将数据发送至下游
        collector.collect(result.toJSONString());
    }

    private JSONObject parseStruct(Struct valueStruct) {
        if (valueStruct == null) return null;

        JSONObject dataJson = new JSONObject();
        for (Field field : valueStruct.schema().fields()) {
            Object v = valueStruct.get(field);
            String type = field.schema().name();
            Object val = null;

            if (v instanceof Long) {
                long vl = (Long) v;
                val = convertLongToTime(vl, type);
            } else if (v instanceof Integer){
                int iv = (Integer) v;
                val = convertIntToDate(iv, type);
            } else if (v == null) {
                val = null;
            } else {
                val = convertObjToTime(v, type);
            }
            dataJson.put(field.name().toLowerCase(), val);
        }
        return dataJson;
    }

    private Object convertObjToTime(Object obj, String type) {
        Object val = obj;
        if (Time.SCHEMA_NAME.equals(type) || MicroTime.SCHEMA_NAME.equals(type) || NanoTime.SCHEMA_NAME.equals(type)) {
            val = java.sql.Time.valueOf(TemporalConversions.toLocalTime(obj)).toString();
        } else if (Timestamp.SCHEMA_NAME.equals(type) || MicroTimestamp.SCHEMA_NAME.equals(type) || NanoTimestamp.SCHEMA_NAME.equals(type) || ZonedTimestamp.SCHEMA_NAME.equals(type)) {
            val = java.sql.Timestamp.valueOf(TemporalConversions.toLocalDateTime(obj, ZoneId.of(serverTimeZone))).toString();
        }
        return val;
    }

    private Object convertIntToDate(int obj, String type) {
        SchemaBuilder date_schema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Date");
        Object val = obj;
        if (Date.SCHEMA_NAME.equals(type)) {
            val = org.apache.kafka.connect.data.Date.toLogical(date_schema, obj).toInstant().atZone(ZoneId.of(serverTimeZone)).toLocalDate().toString();
        }
        return val;
    }

    private Object convertLongToTime(long obj, String type) {
        SchemaBuilder time_schema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Time");
        SchemaBuilder date_schema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Date");
        SchemaBuilder timestamp_schema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Timestamp");
        Object val = obj;
        if (Time.SCHEMA_NAME.equals(type)) {
            val = org.apache.kafka.connect.data.Time.toLogical(time_schema, (int)obj).toInstant().atZone(ZoneId.of(serverTimeZone)).toLocalTime().toString();
        } else if (MicroTime.SCHEMA_NAME.equals(type)) {
            val = org.apache.kafka.connect.data.Time.toLogical(time_schema, (int)(obj / 1000)).toInstant().atZone(ZoneId.of(serverTimeZone)).toLocalTime().toString();
        } else if (NanoTime.SCHEMA_NAME.equals(type)) {
            val = org.apache.kafka.connect.data.Time.toLogical(time_schema, (int)(obj / 1000 / 1000)).toInstant().atZone(ZoneId.of(serverTimeZone)).toLocalTime().toString();
        } else if (Timestamp.SCHEMA_NAME.equals(type)) {
            LocalDateTime t = org.apache.kafka.connect.data.Timestamp.toLogical(timestamp_schema, obj).toInstant().atZone(ZoneId.of(serverTimeZone)).toLocalDateTime();
            val = java.sql.Timestamp.valueOf(t).toString();
        } else if (MicroTimestamp.SCHEMA_NAME.equals(type)) {
            LocalDateTime t = org.apache.kafka.connect.data.Timestamp.toLogical(timestamp_schema, obj / 1000).toInstant().atZone(ZoneId.of(serverTimeZone)).toLocalDateTime();
            val = java.sql.Timestamp.valueOf(t).toString();
        } else if (NanoTimestamp.SCHEMA_NAME.equals(type)) {
            LocalDateTime t = org.apache.kafka.connect.data.Timestamp.toLogical(timestamp_schema, obj / 1000 / 1000).toInstant().atZone(ZoneId.of(serverTimeZone)).toLocalDateTime();
            val = java.sql.Timestamp.valueOf(t).toString();
        } else if (Date.SCHEMA_NAME.equals(type)) {
            val = org.apache.kafka.connect.data.Date.toLogical(date_schema, (int)obj).toInstant().atZone(ZoneId.of(serverTimeZone)).toLocalDate().toString();
        }
        return val;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

