package cn.doitedu.demo6.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Model2FireEvent {
    private String event_id;
    private String pro_name;
    private String pro_value;
}
