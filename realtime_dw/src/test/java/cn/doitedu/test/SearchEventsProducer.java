package cn.doitedu.test;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SearchEventsProducer {

    public static void main(String[] args) throws InterruptedException {


        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"doitedu:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String[] events = {"search","search_return","search_click"};
        while(true){
            int userId = RandomUtils.nextInt(1,100000000);
            long ts = System.currentTimeMillis();
            String event = events[RandomUtils.nextInt(0,3)];

            String json = "{\"user_id\":"+userId+",\"event_id\":\" "+event+" \",\"event_time\":"+ts+",\"properties\":{\"keyword\":\"usb 移动固态\",\"search_id\":\"sc01\",\"res_cnt\":10,\"item_id\":\"000\"}}\n";

            ProducerRecord<String, String> record = new ProducerRecord<>("dwd_events", json);
            producer.send(record);

            Thread.sleep(2);
        }




    }
}
