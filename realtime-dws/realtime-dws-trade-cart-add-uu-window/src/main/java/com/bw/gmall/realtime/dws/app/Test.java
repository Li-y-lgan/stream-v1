package com.bw.gmall.realtime.dws.app;

import com.bw.stream.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.bw.gmall.realtime.dws.app.Test
 * @Author li.yan
 * @Date 2025/5/8 18:26
 * @description:
 */
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);


        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_cart_add", "ckAndGroupId");
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        kafkaStrDS.print();

        env.execute();

    }
}
