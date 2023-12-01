package com.cdc;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.util.Properties;

public class FlinkSourceUtil {

    private static final long DEFAULT_HEARTBEAT_MS = Duration.ofMinutes(5).toMillis();

    /**
     *
     * @param database      库名
     * @param schemaList    schema列表
     * @param tableList     表名列表
     * @param slotName      slot插槽名称
     * @param hostname      数据库地址
     * @param port          端口
     * @param username      账户
     * @param password      密码
     * @param publicationName   所监控的发布名
     * @return
     */
    public static SourceFunction getPGSource(String database,
                                             String schemaList,
                                             String tableList,
                                             String slotName,
                                             String hostname,
                                             int port,
                                             String username,
                                             String password,
                                             String publicationName
    ) {
        Properties properties = new Properties();
        properties.setProperty("snapshot.mode", "always"); // always：全量+增量  never:增量
        properties.setProperty("debezium.slot.name", slotName);
        //properties.setProperty("scan.startup.mode", "earliest-offset");
        //在作业停止后自动清理 slot
        /*properties.setProperty("debezium.slot.drop.on.stop", "true");
        properties.setProperty("include.schema.changes", "true");*/
        //使用连接器配置属性启用定期心跳记录生成
        properties.setProperty("heartbeat.interval.ms", String.valueOf(DEFAULT_HEARTBEAT_MS));

        // 设置架构名、表名
        properties.setProperty("schema.include.list", schemaList);
        properties.setProperty("table.include.list", tableList);
        // 所监控的发布名
        properties.setProperty("publication.name", publicationName);

        // PostGres 数据库
        SourceFunction<String> sourceFunction =
                PostgreSQLSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .database(database) // monitor postgres database
                .schemaList(schemaList)  // monitor inventory schema
                .tableList(tableList) // monitor products table 支持正则表达式
                .username(username)
                .password(password)
                .decodingPluginName("pgoutput")
                .slotName(slotName)
                .deserializer(new MyDebezium()) // converts SourceRecord to JSON String
                //.deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .debeziumProperties(properties)
                .build();
        return sourceFunction;
    }




}
