package cn.doitedu.demo5;

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
