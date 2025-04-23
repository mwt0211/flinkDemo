package function;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojo.User;

public class MyProcessWindowFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {

    @Override
    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
        long start = context.window().getStart();
        long end = context.window().getEnd();
        String windowStart = DateFormatUtils.format(start,"yyyy-MM-dd HH:mm:ss.SSS");
        String windowEnd = DateFormatUtils.format(end,"yyyy-MM-dd HH:mm:ss.SSS");

        long l = elements.spliterator().estimateSize();
        System.out.println("存储的数据条数为 " + l);
        out.collect("key="+s+"的窗口，其开始时间为:【"+windowStart+"，结束时间为:"+windowEnd+"】"+"数据共有： "+l+"条");


    }
}
