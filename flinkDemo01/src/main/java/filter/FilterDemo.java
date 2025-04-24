package filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.User;

public class FilterDemo {
    /**
     * 筛选出年龄大于18的人
     *
     * */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<User> userDs = env.fromElements(User.builder().address("dizhiyi1").age(18).build(),User.builder().address("dizhisasasa").age(15).build(), User.builder().address("低质3").age(20).build());
        userDs.filter(new FilterFunction<User>() {
            @Override
            public boolean filter(User value) throws Exception {
            return value.getAge()>18;

            }
        }).print();
        env.execute();
    }
}
