package state;

import function.WaterSentorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojo.WaterSentor;

import java.time.Duration;
import java.util.Properties;

/**
 * todo:统计每种传感器的平均水位
 */
public class MapStateDemo2 {
    public static void main(String[] args) throws Exception {
        /**
         *
         * 水位线
         * */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker地址
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        //从最新处消费
        consumer.setStartFromLatest();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.addSource(consumer);
        SingleOutputStreamOperator<WaterSentor> waterSentor = stream.map(new WaterSentorMapFunction());
        SingleOutputStreamOperator<WaterSentor> WaterSentorDs = waterSentor.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSentor>forBoundedOutOfOrderness(
                                Duration.ofSeconds(3)).
                        withTimestampAssigner(((element, recordTimestamp) -> element.getVc() * 1000L)));
        //按照Id分组
        SingleOutputStreamOperator<String> process = WaterSentorDs
                .keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSentor, String>() {
                    AggregatingState<Integer,Double> vcAggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //Tuple2<Integer,Integer>  f0为和，f1为个数
                        vcAggregatingState=getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>
                                ("vcAggregatingState",
                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                            @Override
                                            public Tuple2<Integer, Integer> createAccumulator() {
                                                //Tuple2<Integer,Integer>  f0为和，f1为个数
                                                return Tuple2.of(0,0);
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                //Tuple2<Integer,Integer>  f0为和，f1为个数
                                                return Tuple2.of(accumulator.f0+value,accumulator.f1+1);
                                            }

                                            @Override
                                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                //如果分子为Integer 类型，则*1D为Float 类型
                                                return  accumulator.f0*1D/accumulator.f1;
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                return null;
                                            }
                                        }, Types.TUPLE(Types.INT, Types.INT)));
                    }

                    @Override
                    public void processElement(WaterSentor value, KeyedProcessFunction<String, WaterSentor, String>.Context ctx, Collector<String> out) throws Exception {
                        vcAggregatingState.add(value.getVc());

                        out.collect("传感器为【"+value.getId()+"】"+"平均水位值为："+vcAggregatingState.get());
                    }
                });
        process.print();
        env.execute();


    }


}
