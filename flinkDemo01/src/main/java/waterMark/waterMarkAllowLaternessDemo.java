package waterMark;

import function.UserMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojo.User;

import java.time.Duration;
import java.util.Properties;

/**
 * 允许迟到时间
 *
 * */
public class waterMarkAllowLaternessDemo {
    public static void main(String[] args) throws Exception {
        /**
         *
         * 水位线
         * */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker地址
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "test",
                new SimpleStringSchema(),
                properties);
        //从最新处消费
        consumer.setStartFromLatest();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.addSource(consumer);
        SingleOutputStreamOperator<User> userDs = stream.map(new UserMapFunction());

        //todo:指定waterMark策略
        WatermarkStrategy<User> userWatermarkStrategy = WatermarkStrategy
                //指定waterMar生成器
                .<User>forBoundedOutOfOrderness(Duration.ofSeconds(3))//指定乱序流，有等待时间
                //指定时间戳分配器
                .withTimestampAssigner(new SerializableTimestampAssigner<User>() {
            @Override
            public long extractTimestamp(User element, long recordTimestamp) {
//                System.out.println("数据 " + element+"=====>  时间戳： "+recordTimestamp);
                return element.getAge() * 1000l;
            }
        });
        SingleOutputStreamOperator<User> userDsWaterMark = userDs.assignTimestampsAndWatermarks(userWatermarkStrategy);
        KeyedStream<User, String> userKs = userDsWaterMark.keyBy(User::getName);
       //侧输出流标签
        OutputTag<User> userlaterTag = new OutputTag<>("latest-data(User)", Types.POJO(User.class));

        WindowedStream<User, String, TimeWindow> userWs = userKs.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //允许推迟2秒关窗，延迟关窗时间，此刻相当于当
                //todo:关窗事件的触发条件：
                // 事件时间达到 等待时间+推迟时间+水位线才会触发关窗操作
                .allowedLateness(Time.seconds(2))
                //关窗后迟到的数据输出到测输出流中，一般用于异常数据统计
                .sideOutputLateData(userlaterTag)


                ;



        SingleOutputStreamOperator<String> process = userWs.process(new ProcessWindowFunction<User, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<User, String, String, TimeWindow>.Context context, Iterable<User> elements, Collector<String> out) throws Exception {
                long start = context.window().getStart();
                long end = context.window().getEnd();
                String windowStart = DateFormatUtils.format(start,"yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(end,"yyyy-MM-dd HH:mm:ss.SSS");

                long l = elements.spliterator().estimateSize();
//                System.out.println("存储的数据条数为 " + l);
                out.collect("key="+s+"的窗口，其开始时间为:【"+windowStart+"，结束时间为:"+windowEnd+"】"+"数据共有： "+l+"条");


            }
        });
//输出主输出流数据
        process.print();
        //侧输出流输出数据
        process.getSideOutput(userlaterTag).printToErr();
        env.execute();


    }
}
