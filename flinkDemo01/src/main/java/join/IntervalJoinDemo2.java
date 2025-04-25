package join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;
import java.util.Properties;

/**
 * 双流连接的迟到的数据处理
 */
public class IntervalJoinDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker地址
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        //从最新处消费
        consumer.setStartFromLatest();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.addSource(consumer);
        //输入String,输出二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], Integer.valueOf(split[1]));
            }
        }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                //乱序
                        .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L));

//创建无界流2，消费kafka的topic为   test-stream2
        Properties properties2 = new Properties();
        properties2.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker地址
        FlinkKafkaConsumer<String> consumer2 = new FlinkKafkaConsumer<>("test-stream2", new SimpleStringSchema(), properties2);
        //从最新处消费
        consumer2.setStartFromLatest();

        DataStreamSource<String> stream2 = env.addSource(consumer2);

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = stream2.map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], Integer.valueOf(split[1]), Integer.valueOf(split[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                .<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((value, ts) -> value.f1 * 1000L));
        //todo:Interval join
        //分别keyBy,key为关联条件
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);
        OutputTag<Tuple2<String, Integer>> ks1LaterDataTag = new OutputTag<>("ks1-later-data", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> ks2LaterDataTag = new OutputTag<>("ks2-later-data", Types.TUPLE(Types.STRING, Types.INT, Types.INT));
        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2)
                //往前偏两秒，往后偏2秒，为闭区间
                .between(Time.seconds(-2), Time.seconds(2))
                //左侧迟到的数据放到在侧输出流中
                .sideOutputLeftLateData(ks1LaterDataTag)
                //右侧迟到的数据放到在侧输出流中
                .sideOutputRightLateData(ks2LaterDataTag)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "<---->" + right);
                    }
                });
        process.print("主流");
        process.getSideOutput(ks1LaterDataTag).printToErr("ks1-later-data");
        process.getSideOutput(ks2LaterDataTag).printToErr("ks2-later-data");


        env.execute();
    }
}
