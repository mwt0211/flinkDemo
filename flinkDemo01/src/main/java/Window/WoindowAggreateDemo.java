package Window;

import function.UserMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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

import java.sql.Date;
import java.util.Properties;

public class WoindowAggreateDemo {
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
        SingleOutputStreamOperator<String> aggregate = userWs.aggregate(new AggregateFunction<User, Integer, String>() {
            /**
             *
             * new AggregateFunction<User, Integer, String>()
             * User：输入类型
             * Integer：参与累加的类型
             * String：输出类型
             * */

            @Override
            public Integer createAccumulator() {
                /**
                 * 初始化累加器
                 * */
                System.out.println("创建累加器 = ");
                return 0;
            }

            @Override
            public Integer add(User value, Integer accumulator) {
                System.out.println("计算逻辑 = "+value.getAge()+accumulator);
                return value.getAge()+accumulator;
            }

            @Override
            public String getResult(Integer accumulator) {
                System.out.println("获取最终结果，窗口输出 = "+accumulator);
                return "最终结果的输出为："+accumulator;
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                //一般用于会话窗口
                return null;
            }
        });

        System.out.println("调用aggregate方法 = *******【start】***");
        aggregate.print();
        System.out.println("调用aggregate方法 = *******  【 end 】  ***");

        //窗口函数

        env.execute();

    }
}
