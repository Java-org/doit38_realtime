package cn.doitedu.demo6_doit39.beans;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Model2OnlineProfile {
    private String[] event_seq;
    private int seq_count;
}
