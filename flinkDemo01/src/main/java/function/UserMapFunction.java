package function;

import org.apache.flink.api.common.functions.MapFunction;
import pojo.User;

public class UserMapFunction implements MapFunction<String, User> {
    @Override
    public User map(String value) throws Exception {
        String[] split = value.split(",");
        User user = new User();
//        user.setId(split[0]);
        user.setName(split[0]);
        user.setAge(Integer.valueOf(split[1]));
//        user.setAddress(split[3]);
//        user.setEmail(split[4]);

        return user;
    }
}
