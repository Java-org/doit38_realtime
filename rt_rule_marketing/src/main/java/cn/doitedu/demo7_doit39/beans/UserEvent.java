package cn.doitedu.demo7_doit39.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class UserEvent {

    private long user_id;
    private long event_time;
    private String event_id;
    private Map<String,String> properties;

}
