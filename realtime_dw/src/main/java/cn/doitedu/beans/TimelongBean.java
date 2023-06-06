package cn.doitedu.beans;

import lombok.*;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class TimelongBean {

    // 源数据json中就包含的字段
    private long user_id;
    private String device_type;
    private String release_channel;
    private String session_id;
    private String page_type;
    private String page_url;
    private String event_id;
    private long event_time;
    private Map<String,String> properties;

    // 要计算填充的
    private long page_start_time = -1;
    private long page_end_time = -1;

}
