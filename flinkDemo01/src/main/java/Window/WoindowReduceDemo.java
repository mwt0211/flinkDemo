package Window;

import function.UserMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojo.User;


import java.sql.Date;
import java.util.Properties;

public class WoindowReduceDemo {
    public static void main(String[] args) throws Exception {

        /**
         * 窗口增量聚合
         *
         * 1，相同数据来的时候第一条，不会调用reduce方法
         * 2.增量聚合，来一条数据会参与计算，但是不会输出
         * 3.在窗口触发的时候，才会输出总的计算结果
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
        //窗口分配器
        //1.1 无KeyBy,所有的数据进入到同一个子任务，并行度只能为1
//        userDs.windowAll()

        //1.2有keyBy
        //1.2基于时间的
        //1.21滚动窗口，时间为10秒

        /**
         * 窗口的reduce
         * 1：
         * */
        WindowedStream<User, String, TimeWindow> userWs = userKs.window(TumblingProcessingTimeWindows.of(Time.seconds(12)));

        SingleOutputStreamOperator<User> reduce = userWs.reduce(new ReduceFunction<User>() {
            @Override
            public User reduce(User value1, User value2) throws Exception {
                //实现两个用户的年龄相加
                int newAge = value1.getAge() + value2.getAge();
                System.out.println("调用reduce方法 = **********");
                System.out.println("value1 = " + value1);
                System.out.println("value2 = " + value2);
                return new User(value1.getId(), value1.getName(), newAge, "", "", new Date(System.currentTimeMillis()));
            }
        });
        System.out.println("调用reduce方法 = *******【start】***");
        reduce.print();
        System.out.println("调用reduce方法 = *******  【 end 】  ***");

        //窗口函数

        env.execute();

    }
}
