package cn.doitedu.beans;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class SearchAggBean {
    long user_id;
    String keyword;
    String search_id;
    long search_time;
    long return_item_count;
    long click_item_count;
}
