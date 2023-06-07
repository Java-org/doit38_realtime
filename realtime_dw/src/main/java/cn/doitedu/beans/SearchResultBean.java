package cn.doitedu.beans;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class SearchResultBean {
    long user_id;
    String keyword;
    String split_words;
    String similar_word;
    String search_id;
    long search_time;
    long return_item_count;
    long click_item_count;
}
