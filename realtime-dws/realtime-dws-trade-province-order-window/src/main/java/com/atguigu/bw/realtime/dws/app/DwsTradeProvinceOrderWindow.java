package com.atguigu.bw.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.stream.realtime.common.base.BaseApp;
import com.bw.stream.realtime.common.bean.TradeProvinceOrderBean;
import com.bw.stream.realtime.common.constant.Constant;
import com.bw.stream.realtime.common.function.BeanToJsonStrMapFunction;
import com.bw.stream.realtime.common.function.DimAsyncFunction;
import com.bw.stream.realtime.common.util.DateFormatUtil;
import com.bw.stream.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.atguigu.bw.realtime.dws.app.DwsTradeProvinceOrderWindow
 * @Author li.yan
 * @Date 2025/5/2 15:21
 * @description:
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwsTradeProvinceOrderWindow().start(
                10031,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 1. 过滤空消息并转换类型
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        jsonObjDS.print();

        // 2. 按键分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // 3. 去重处理
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

        // 4. 指定Watermark
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<JSONObject>) (jsonObj, recordTimestamp) ->
                                        jsonObj.getLong("ts") * 1000
                        )
        );

        // 5. 转换为Bean对象（修复点1：添加returns）
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                jsonObj -> {
                    String provinceId = jsonObj.getString("province_id");
                    BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                    Long ts = jsonObj.getLong("ts");
                    String orderId = jsonObj.getString("order_id");

                    return TradeProvinceOrderBean.builder()
                            .provinceId(provinceId)
                            .orderAmount(splitTotalAmount)
                            .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                            .ts(ts)
                            .build();
                }
        ).returns(TradeProvinceOrderBean.class);  // 显式声明返回类型

        // 6. 按省份分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        //TODO 7.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 8. 聚合（修复点2：使用匿名类代替Lambda）
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input,
                                      Collector<TradeProvinceOrderBean> out) {
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );

        // 9. 关联省份维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public void addDims(TradeProvinceOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean orderBean) {
                        return orderBean.getProvinceId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 10. 写入Doris
        withProvinceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }
}