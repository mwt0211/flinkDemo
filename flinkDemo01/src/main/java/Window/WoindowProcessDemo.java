package Window;

import function.UserMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojo.User;

import java.util.Properties;

public class WoindowProcessDemo {
    public static void main(String[] args) throws Exception {

        /**
         * 窗口增量聚合
         *
         * 1，第一条数据来创建累加器
         * 2.增量聚合，来一条数据会参与计算，但是不会输出
         * 3.在窗口触发的时候，才会输出总的计算结果
         * 4.输入，输出类型可以不一致
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
        KeyedStream<User, String> userKs = userDs.keyBy(User::getName);
        /**
         * 窗口的reduce
         * 1：
         * */
        WindowedStream<User, String, TimeWindow> userWs = userKs.window(TumblingProcessingTimeWindows.of(Time.seconds(12)));
        SingleOutputStreamOperator<String> process = userWs.process(new ProcessWindowFunction<User, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<User, String, String, TimeWindow>.Context context, Iterable<User> elements, Collector<String> out) throws Exception {
                long start = context.window().getStart();
                long end = context.window().getEnd();
                String windowStart = DateFormatUtils.format(start,"yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(end,"yyyy-MM-dd HH:mm:ss.SSS");

                long l = elements.spliterator().estimateSize();
                System.out.println("存储的数据条数为 " + l);
                out.collect("key="+s+"的窗口，其开始时间为:【"+windowStart+"，结束时间为:"+windowEnd+"】"+"数据共有： "+l+"条");


            }
        });


        System.out.println("process = *******【start】***");
        process.print();
        System.out.println("process = *******  【 end 】  ***");

        //窗口函数

        env.execute();

    }
}
