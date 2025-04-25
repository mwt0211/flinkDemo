package filter;

import function.UserMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojo.User;

import java.util.Properties;

public class FilterDemo {
    /**
     * 筛选出年龄大于18的人
     *
     * */
    public static void main(String[] args) throws Exception {
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
        //todo:改成从kafka中接收数据，然后解析数据，筛选出年龄大于18的人的信息

        SingleOutputStreamOperator<User> userDs = stream.map(new UserMapFunction());
        userDs.filter(new FilterFunction<User>() {
            @Override
            public boolean filter(User value) throws Exception {
            return value.getAge()>18;

            }
        }).print();
        env.execute();
    }
}
