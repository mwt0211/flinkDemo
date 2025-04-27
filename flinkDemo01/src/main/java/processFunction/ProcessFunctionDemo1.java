package processFunction;

import function.UserMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojo.User;

import java.time.Duration;
import java.util.*;

/**
 * 计算每个窗口结果的前两名
 */
public class ProcessFunctionDemo1 {
    public static void main(String[] args) throws Exception {
        /**
         *
         * 水位线
         * */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker地址
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        //从最新处消费
        consumer.setStartFromLatest();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.addSource(consumer);
        SingleOutputStreamOperator<User> userDs = stream.map(new UserMapFunction());
        userDs = userDs.assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(((element, recordTimestamp) -> element.getAge() * 1000L)));
        //todo:1 按照age分组，开窗聚合（增量计算+全量打标签）
        //窗口开窗聚合之后就是普通的流，没有窗口信息，需要自己打上窗口信息，即tuple的第三个元素
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> userKs = userDs.keyBy(new KeySelector<User, Integer>() {
            @Override
            public Integer getKey(User value) throws Exception {
                return value.getAge();
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))).aggregate(new ageCount(), new myResult());
        //todo:2 根据第一步输出的结果，按照窗口结束事件做keyBy
        SingleOutputStreamOperator<String> process = userKs.keyBy(s -> s.f2).process(new TopN(2));
        process.print();
        env.execute();


    }
    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> dataMaps;
        //要获取Top的数量，例如前两名，则传2
        private int threshold;

        public TopN(int threshold) {
            this.threshold = threshold;
            dataMaps=new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            //进入这个方法的，只是一条数据。需要将不同窗口的数据收集起来，然后将不同窗口的数据排序
            //如果不包含,则放进去
            Long windowEnd = value.f2;
            if(!dataMaps.containsKey(windowEnd)){
            List<Tuple3<Integer, Integer, Long>> dataList = new ArrayList<>();
                dataList.add(value);
                dataMaps.put(windowEnd,dataList);
            }else {
                //如果包含，则加进去
                List<Tuple3<Integer, Integer, Long>> tuple3s = dataMaps.get(windowEnd);
                tuple3s.add(value);
            }
            //注册定时器,windowEnd+1ms
            ctx.timerService().registerEventTimeTimer(windowEnd+1);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            //1.定时器触发，同一个窗口的结果排序
            Long currentKey = ctx.getCurrentKey();
            //Tuple3<age, count, windowEnd>
            List<Tuple3<Integer, Integer, Long>> windowDataList = dataMaps.get(currentKey);
            windowDataList.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    return o2.f1- o1.f1;
                }
            });
            //2. 取TopN
            StringBuilder sb = new StringBuilder();
            sb.append("*********   start     ****\n");
            Tuple3<Integer, Integer, Long> data=null;
            for (int i = 0; i < Math.min(threshold,windowDataList.size()); i++) {
                data= windowDataList.get(i);
                sb.append("Top 【 "+(i+1)+" 】\n");
                sb.append("age ="+data.f0+"\n");
                sb.append("count ="+data.f1+"\n");

            }
            sb.append("窗口i结束时间为：【 " + DateFormatUtils.format(data.f2,"yyyy-MM-dd HH-mm:ss.SSS")+" 】\n");
            sb.append("******  end    *******\n");
            //用完的list需要清理
            windowDataList.clear();
            out.collect(sb.toString());

        }
    }

    public static class ageCount implements AggregateFunction<User, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(User value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }


    public static class myResult extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {
        @Override
        public void process(Integer key, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            long end = context.window().getEnd();
            Integer count = elements.iterator().next();
            out.collect(Tuple3.of(key, count, end));
        }
    }

}
