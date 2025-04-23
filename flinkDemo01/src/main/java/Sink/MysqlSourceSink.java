package Sink;

import function.UserMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojo.User;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.Properties;
import java.util.UUID;

public class MysqlSourceSink {
    public static void main(String[] args) throws Exception {
/**
 * 输出到jdbc中
 * */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker地址
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group"); // 消费者组ID
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 从最早的消息开始消费



        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "test",
                new SimpleStringSchema(),
                properties);
        //从最新处消费
        consumer.setStartFromLatest();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.addSource(consumer);

        String sql="INSERT INTO `exer_flink`.`flink_test`(`id`, `name`, `age`, `address`, `email`, `create_time`)" +
                " VALUES (?, ?, ?, ?, ?, ?);";


        //todo:改成从kafka中接收数据，然后写到数据库中
        //todo:待解决从文件中拿取数据
        //改成从kafka中读取消息，然后入库
        SingleOutputStreamOperator<User> userDs = stream.map(new UserMapFunction());
        System.out.println("userDs = " + userDs);
        SinkFunction<User> jdbcSink = JdbcSink.sink(sql, new JdbcStatementBuilder<User>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, User user) throws SQLException {
                        System.out.println("user = " + user);
                        preparedStatement.setString(1, UUID.randomUUID().toString().replace("-",""));
                        preparedStatement.setString(2, user.getName());
                        preparedStatement.setInt(3, user.getAge());
                        preparedStatement.setString(4, user.getAddress());
                        preparedStatement.setString(5, user.getEmail());
                        preparedStatement.setDate(6, new Date(System.currentTimeMillis()));
                    }
                },
                JdbcExecutionOptions.builder()
                        //重试次数
                        .withMaxRetries(3)
                        .withBatchIntervalMs(10)
                        .withBatchSize(100)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:mysql://192.168.150.129:3306/exer?serverTimezone=UTC")
                        .withUrl("jdbc:mysql://localhost:3306/exer_flink?serverTimezone=UTC")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root"
                        )
                        .withPassword("root")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build());
        userDs.addSink(jdbcSink);
        env.execute();
    }
}
