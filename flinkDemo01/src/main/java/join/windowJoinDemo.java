package join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
/**
 * 窗口链接
 * */
public class windowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(

                Tuple2.of("a", 2),
                Tuple2.of("b", 3),
                Tuple2.of("c", 1),
                Tuple2.of("d", 2),
                Tuple2.of("e", 5),
                Tuple2.of("e", 5),
                Tuple2.of("f", 5)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L));


        SingleOutputStreamOperator<Tuple3<String, Integer,Integer>> ds2 = env.fromElements(

                Tuple3.of("a", 2,11),
                Tuple3.of("b", 2,22),
                Tuple3.of("c", 3,33),
                Tuple3.of("d", 4,11),
                Tuple3.of("e", 5,11),
                Tuple3.of("e", 6,14),
                Tuple3.of("g", 7,15)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer,Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        DataStream<String> join = ds1.join(ds2)
                .where(r1 -> r1.f0)//ds1的KeyBy
                .equalTo(r2 -> r2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {

                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                        return first + "<----->" + second;
                    }
                });
/**
 * 落在同一个窗口，keyBy的key相同才能匹配
 * */
        join.print();
        env.execute();
    }
}
