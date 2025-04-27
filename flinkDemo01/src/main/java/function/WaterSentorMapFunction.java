package function;

import org.apache.flink.api.common.functions.MapFunction;
import pojo.User;
import pojo.WaterSentor;

public class WaterSentorMapFunction implements MapFunction<String, WaterSentor> {
    @Override
    public WaterSentor map(String value) throws Exception {
        String[] split = value.split(",");
        WaterSentor waterSentor = new WaterSentor();
        waterSentor.setId(split[0]);
        waterSentor.setVc(Integer.valueOf(split[1]));
        waterSentor.setCount(Integer.valueOf(split[2]));
//        user.setAddress(split[2]);
//        user.setEmail(split[3]);

        return waterSentor;
    }
}
