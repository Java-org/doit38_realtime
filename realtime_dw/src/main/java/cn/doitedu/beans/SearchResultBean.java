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

    public void set(long user_id, String keyword, String split_words, String similar_word, String search_id, long search_time, long return_item_count, long click_item_count) {
        this.user_id = user_id;
        this.keyword = keyword;
        this.split_words = split_words;
        this.similar_word = similar_word;
        this.search_id = search_id;
        this.search_time = search_time;
        this.return_item_count = return_item_count;
        this.click_item_count = click_item_count;
    }
}
