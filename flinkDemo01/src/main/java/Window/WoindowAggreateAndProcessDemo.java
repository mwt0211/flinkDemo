package Window;

import function.MyAggregateFunction;
import function.MyProcessWindowFunction;
import function.UserMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojo.User;

import java.util.Properties;

public class WoindowAggreateAndProcessDemo {
    public static void main(String[] args) throws Exception {

        /**
         * 窗口增量聚合 +全窗口 process
         *
         * 1，增量聚合函数来一条计算一条
         *
         * 3.在窗口触发的时候，增量聚合的结果，传递给全窗口函数
         * 4.全窗口函数处理数据之后，输出
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
         * 窗口的aggregate
         * 1：
         * */
        WindowedStream<User, String, TimeWindow> userWs = userKs.window(TumblingProcessingTimeWindows.of(Time.seconds(12)));
        SingleOutputStreamOperator<String> aggregate = userWs.aggregate(new MyAggregateFunction(),new MyProcessWindowFunction());
        System.out.println("调用aggregate方法 = *******【start】***");
        aggregate.print();
        System.out.println("调用aggregate方法 = *******  【 end 】  ***");

        //窗口函数

        env.execute();

    }

}
