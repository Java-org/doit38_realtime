package cn.doitedu.demo6.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;


@Data
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Model2Param {
    String rule_id;
    List<Model2OfflineProfileCondition> offline_profile;
    Model2OnlineProfile online_profile;
    Model2FireEvent fire_event;
}
