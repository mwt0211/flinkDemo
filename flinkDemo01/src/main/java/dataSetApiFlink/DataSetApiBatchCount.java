package dataSetApiFlink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class DataSetApiBatchCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataSource<String> dataSource = env.readTextFile("src/input/hello.txt");
        //按行切分，转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordAsOne = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //按照空格切分
                String[] word = s.split(" ");
                for (String s1 : word) {
                    Tuple2<String, Integer> wordTiple2 = Tuple2.of(s1, 1);
                    //使用采集器，向下游发送
                    collector.collect(wordTiple2);
                }

            }
        });
        //按照单词分组
        UnsortedGrouping<Tuple2<String, Integer>> wordGroupBy = wordAsOne.groupBy(0);//是索引
        //各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> res = wordGroupBy.sum(1);//1是位置，表示第二个元素
        //输出
      res.print();

    }
}
