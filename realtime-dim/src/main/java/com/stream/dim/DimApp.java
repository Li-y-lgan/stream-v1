package com.stream.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.stream.realtime.common.base.BaseApp;
import com.bw.stream.realtime.common.bean.TableProcessDim;
import com.bw.stream.realtime.common.constant.Constant;
import com.bw.stream.realtime.common.util.FlinkSourceUtil;
import com.bw.stream.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import funtion.HBaseSinkFunction;
import funtion.TableProcessFunction;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;


/**
 * @Package com.stream.dim.DimApp
 * @Author li.yan
 * @Date 2025/4/9 9:11
 * @description:
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @SneakyThrows
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDs = etl(kafkaStrDS);
        SingleOutputStreamOperator<TableProcessDim> tpDs = readTableProcess(env);
//
        tpDs = createHbaseTable(tpDs);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDs, tpDs);

        dimDS.print();
        writeToHBase(dimDS);

        env.execute();

    }
    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction());
    }
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDs, SingleOutputStreamOperator<TableProcessDim> tpDs) {
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDs.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDs.connect(broadcastDS);
        return connectDS.process(
                new TableProcessFunction(mapStateDescriptor));
    }
    private SingleOutputStreamOperator<TableProcessDim> createHbaseTable(SingleOutputStreamOperator<TableProcessDim> tpDs) {
        tpDs = tpDs.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseConn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tp) {

                String op = tp.getOp();

                String sinkTable = tp.getSinkTable();

                String[] sinkFamilies = tp.getSinkFamily().split(",");

                if ("d".equals(op)) {
                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                } else if ("r".equals(op) || "c".equals(op)) {
                    HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                } else {
                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                    HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                }
                return tp;
            }
        }).setParallelism(1);
//        tpDs.print();
        return tpDs;
    }
    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall_2025_config", "table_process_dim");
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        return mysqlStrDS.map((MapFunction<String, TableProcessDim>) jsonStr -> {
            JSONObject jsonObj = JSON.parseObject(jsonStr);
            String op = jsonObj.getString("op");
            TableProcessDim tableProcessDim;
            if ("d".equals(op)) {
                tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
            } else {
                tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);

            }
            tableProcessDim.setOp(op);
            return tableProcessDim;
        }
        ).setParallelism(1);
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
                                                                                  @Override
                                                                                  public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                                                                                      JSONObject jsonObj = JSON.parseObject(jsonStr);
                                                                                      String db = jsonObj.getString("database");
                                                                                      String type = jsonObj.getString("type");
                                                                                      String data = jsonObj.getString("data");
                                                                                      if ("gmall_2025".equals(db)
                                                                                              && ("insert".equals(type)
                                                                                              || "update".equals(type)
                                                                                              || "delete".equals(type)
                                                                                              || "bootstrap-insert".equals(type))
                                                                                              && data != null
                                                                                              && data.length() > 2
                                                                                      ) {
                                                                                          out.collect(jsonObj);
                                                                                      }
                                                                                  }
                                                                              }
        );
        jsonObjDs.print();
        return jsonObjDs;
    }
}








