package state;

import function.WaterSentorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
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
 * todo:统计每种传感器每种水位出现的次数
 */
public class MapStateDemo1 {
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
        SingleOutputStreamOperator<String> process = WaterSentorDs.keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSentor, String>() {
                    MapState<Integer, Integer> vcCountMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("vcCountMapState", Types.INT, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSentor value, KeyedProcessFunction<String, WaterSentor, String>.Context ctx, Collector<String> out) throws Exception {
                        //判断是否存在对应的Key
                        Integer vc = value.getVc();
                        if (vcCountMapState.contains(vc)) {
                            Integer integer = vcCountMapState.get(vc);
                            integer = integer + 1;
                            vcCountMapState.put(vc,integer);
                        } else {
                            vcCountMapState.put(vc, 1);
                        }
                        out.collect("水位传感器为:【"+value.getId()+" 】水位线为：【"+value.getVc()+" 】出现的次数为："+vcCountMapState.get(vc));
                    }
                });
        process.print();
        env.execute();


    }


}
