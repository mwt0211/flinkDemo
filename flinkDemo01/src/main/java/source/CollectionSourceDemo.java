package source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionSourceDemo {
    /**
     * 从集合中毒数据
     * */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置一个并行度
        env.setParallelism(1);
//        env.fromCollection(Arrays.asList("1","2","DYL","HPP","MWT")).print();
        env.fromElements("1","2","DYL","HPP","MWT").print();
        env.execute();

    }
}
