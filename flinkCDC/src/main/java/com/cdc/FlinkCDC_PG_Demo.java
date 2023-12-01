package com.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_PG_Demo {
    public static void main(String[] args) {

        // 0、创建 Flink 任务的执行环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",3001);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.enableCheckpointing(1000);
        env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("checkPoint在HDFS上的储存位置");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointTimeout(500);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);

        String tableList = "ida50_pub.svr_pub_bill,"
                + "ida50_pub.svr_pub_task,"
                + "ida50_pub.svr_pub_bill_action,"
                + "ida50_wo.svr_wo_task,"
                + "ida50_wo.svr_wo_bill,"
                + "ida50_sa.svr_sa_task,"
                + "ida50_sa.svr_sa_bill,";


        DataStreamSource<String> pgSource = env.addSource(FlinkSourceUtil.getPGSource("osshjk",
                "ida50_pub,ida50_wo,ida50_sa",
                tableList,
                "pg_cdc",
                "127.0.0.1",
                5432,
                "postgres",
                "M6b!9JaNg3",
                "dbz_publication"
        ));

        pgSource.print();

        try {
            env.execute("Flink CDC PG");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
