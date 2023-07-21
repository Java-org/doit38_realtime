import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class EventsDataMoni {

    public static void main(String[] args) throws InterruptedException {

        for(int i=0;i<7;i++) {
            new Thread(new MyRunnable()).start();
        }


    }


    public static class MyRunnable implements Runnable{

        @Override
        public void run() {
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"doitedu:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.ACKS_CONFIG,"1");

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            String[] events =  new String[]{"a","b","c","d","e","f","g","h","j","w","x"};

            for(int i=0; i< 1000000; i++) {
                String event = events[RandomUtils.nextInt(0,events.length)];
                int uid = RandomUtils.nextInt(0,100);

                producer.send(new ProducerRecord<>("dwd_events", "{\"user_id\":"+uid+",\"event_id\":\""+event+"\",\"properties\":{\"p1\":\"v1\"}}"));
                producer.flush();
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
