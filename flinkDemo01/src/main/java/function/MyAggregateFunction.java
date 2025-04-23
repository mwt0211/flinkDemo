package function;

import org.apache.flink.api.common.functions.AggregateFunction;
import pojo.User;

public class MyAggregateFunction implements AggregateFunction<User, Integer, String> {
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
        System.out.println("计算逻辑 = "+(value.getAge()+accumulator));
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
}
