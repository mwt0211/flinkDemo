package checkPoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class CheckPointDemo01 {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认端口号为8081，启动webUi页面
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker地址
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        //从最新处消费
        consumer.setStartFromLatest();
        env.setParallelism(1);
        //todo:配置检查点相关
        //todo:1 开启检查点，每隔5秒启动检查点保存,默认为精准一次，barry对齐
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        //todo:2 指定检查点存储位置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        checkpointConfig.setCheckpointStorage("file:///C:\\Users\\22162\\Desktop\\checkPointDemo");
        //todo:2.1 指定超时时间,默认值10分钟
        checkpointConfig.setCheckpointTimeout(60000);
        //todo:2.2 指定最大并发数。即同时运行checkPoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //todo:2.3 最小等待时间间隔，即上一轮checkPoint的结束到下一轮checkPoint的开始之间的间隔时间，强制并发为1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        //todo:2.3 取消作业时，数据保留在外部系统
        //todo:2.3.1 取消作业时，，删除作业取消时的外部作业话检查点
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //todo:2.4 容忍检查点失败的故障数量
        checkpointConfig.setTolerableCheckpointFailureNumber(2);



        DataStreamSource<String> stream = env.addSource(consumer);
        stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(value, 1));
                    }
                })
                .setParallelism(1)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();
        env.execute();
    }
}
