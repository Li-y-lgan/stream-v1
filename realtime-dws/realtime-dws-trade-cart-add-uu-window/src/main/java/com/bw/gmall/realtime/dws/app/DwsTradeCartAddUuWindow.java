package com.bw.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.stream.realtime.common.base.BaseApp;
import com.bw.stream.realtime.common.bean.CartAddUuBean;
import com.bw.stream.realtime.common.constant.Constant;
import com.bw.stream.realtime.common.function.BeanToJsonStrMapFunction;
import com.bw.stream.realtime.common.util.DateFormatUtil;
import com.bw.stream.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.bw.gmall.realtime.dws.app.DwsTradeCartAddUuWindow
 * @Author li.yan
 * @Date 2025/5/2 15:08
 * @description:
 */
public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindow().start(
                10024,
                4,
                "dws_trade_cart_add_uu_window",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 1. 转换数据格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 2. 指定Watermark和事件时间
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts") * 1000)
        );

        // 3. 按照用户ID分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(json -> json.getString("user_id"));

        // 4. 状态编程处理独立用户
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> descriptor =
                                new ValueStateDescriptor<>("lastCartDateState", String.class);
                        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        String lastDate = lastCartDateState.value();
                        String currentDate = DateFormatUtil.tsToDate(jsonObj.getLong("ts") * 1000);

                        if (lastDate == null || !lastDate.equals(currentDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(currentDate);
                        }
                    }
                }
        );

        // 5. 开窗处理
        AllWindowedStream<JSONObject, TimeWindow> windowDS =
                cartUUDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 6. 聚合计算（修复类型推断部分）
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                },
                // 使用显式的匿名类替代Lambda表达式
                (AllWindowFunction<Long, CartAddUuBean, TimeWindow>) (window, values, out) -> {
                    Long count = values.iterator().next();
                    String stt = DateFormatUtil.tsToDateTime(window.getStart());
                    String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                    String curDate = DateFormatUtil.tsToDate(window.getStart());

                    out.collect(new CartAddUuBean(stt, edt, curDate, count));
                }
        );

        // 7. 输出到Doris
        aggregateDS.print();
        aggregateDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));
    }
}