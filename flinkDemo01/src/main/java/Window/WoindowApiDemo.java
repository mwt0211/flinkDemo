package Window;

import function.UserMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojo.User;

import java.util.Properties;

public class WoindowApiDemo {
    public static void main(String[] args) throws Exception {

        /**
         * 窗口聚合Api
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
        KeyedStream<User, String> userKs = userDs.keyBy(User::getId);
        //窗口分配器
        //1.1 无KeyBy,所有的数据进入到同一个子任务，并行度只能为1
//        userDs.windowAll()

        //1.2有keyBy
        //1.2基于时间的
        //1.21滚动窗口，时间为10秒
        userKs.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //1.22滑动窗口，时长为10秒，滑动时长为2秒
        userKs.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)));
        //1.23会话窗口，超时时间为5秒
        userKs.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));//会话窗口，超时时间为5秒

        //1.2基于计数的
        userKs.countWindow(5);//滚动窗口，5个数据
        userKs.countWindow(5,2);//滑动窗口，5个元素，滑动步长：2个元素

        //窗口函数

        env.execute();

    }
}
