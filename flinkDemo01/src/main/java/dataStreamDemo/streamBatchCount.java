package dataStreamDemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class streamBatchCount {
    /**
     * 流处理,
     * 输出
     * (flink,1)
     *  (hello,4)
     * */
    public static void main(String[] args) throws Exception {
        //todo:1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo:2  读取文件
        DataStreamSource<String> streamSource = env.readTextFile("src/input/hello.txt ");
        //处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamAsOne = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] va = value.split(" ");
                for (String s : va) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(s, 1);
                    //使用采集器，向下游发送
                    out.collect(tuple2);
                }
            }
        });
        //todo:3  分组
        KeyedStream<Tuple2<String, Integer>, String> wordAsOneKs = streamAsOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        //todo:4  聚合，按位置
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAsOneKs.sum(1);
        //todo:5  输出
        sumDS.print();
        //todo:6  启动
        env.execute();

    }
}
