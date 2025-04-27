package pojo;


import lombok.Builder;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.sql.Date;
import java.util.Objects;

@Builder
@Data
public class WaterSentor {
    /**
     * 传感器Id
     * */
    private String id;
    /**
     * 传感器水位
     * */
    private Integer vc;
    /**
     * 传感器次数
     * */
    private Integer count;

    public WaterSentor() {
    }

    public WaterSentor(String id, Integer vc, Integer count) {
        this.id = id;
        this.vc = vc;
        this.count = count;
    }
}
