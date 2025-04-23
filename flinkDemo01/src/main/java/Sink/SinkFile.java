package Sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.ZoneId;


public class SinkFile {
    /***
     * 将随机生成的数字写到文件当中
     * */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(14);
        //必须开启Checkpoint,否则生成的文件名会为XXXX.inprogress.XXXX,表示在文件中追加信息，无法读取
        //开启Checkpoint,之后生成的文件是正常的
        env.enableCheckpointing(2000,CheckpointingMode.EXACTLY_ONCE);
        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {

                    @Override
                    public String map(Long value) throws Exception {
                        return "输入数字为：" + value;
                    }
                },
                //输出多少个数字
                99999,
//                Long.MAX_VALUE,
                //控制每秒生成数字的速率
                RateLimiterStrategy.perSecond(1000),
                Types.STRING
        );
        DataStreamSource dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        //输出到文件系统
        FileSink<String> sinkToFile = FileSink.<String>forRowFormat(new Path("C:\\Users\\22162\\Downloads\\demoOut"), new SimpleStringEncoder<>("UTF-8"))
                //文件的一些配置
                .withOutputFileConfig(
                        OutputFileConfig
                                .builder()
                                //文件名称
                                .withPartPrefix("SinkToFile")
                                //后缀
                                .withPartSuffix(".log")
                                .build())
                //按照目录分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH", ZoneId.systemDefault()))
                //文件滚动策略
                //按照下面的配置，要么时间到了，要么达到大小，其中任何一个达到都会生成新的文件
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        //滚动事件
                        .withRolloverInterval(Duration.ofSeconds(10))
                        //每个大小
                        .withMaxPartSize(new MemorySize(1024 * 1024 * 3)).build()
                )
                .build();

//
        dataGen.sinkTo(sinkToFile);


        env.execute();
    }
}
