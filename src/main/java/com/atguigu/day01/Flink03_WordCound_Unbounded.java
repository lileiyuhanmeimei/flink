package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_WordCound_Unbounded {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据创建流
        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102",9999);
        //3.将数据压平并转换成元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToDSream = inputDS.flatMap(new MyflatMapper1());
        //4.分组
        KeyedStream<Tuple2<String, Integer>, String> woedToOneDstream = wordToDSream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        //5.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = woedToOneDstream.sum(1);
        //6.打印结果
        result.print("Result");
        //7.启动任务
            env.execute();
    }

    private static class MyflatMapper1 implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
