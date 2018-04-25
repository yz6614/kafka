package producer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
    private final String TEST_TOPIC = "kafka_test_topic";
    public static Producer<String, String> producer;

    /**
     * produce a Kafka Producer object
     *
     * @param broker_list
     * @param producer_type
     */
    public static void proStart(String broker_list, String producer_type) {
        Properties props = new Properties();
        props.put("metadata.broker.list", broker_list);
        props.put("producer.type", producer_type == null ? "sync" : producer_type);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "-1");
        ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<String, String>(producerConfig);
    }

    /**
     * produce the
     *
     * @return
     */
    public static String makeMessage() {
        StringBuilder str = new StringBuilder();
        str.append("zyl--").append("kafka--").append(getDate());
        return str.toString();
    }

    public static void send(String topicName,String message){
        if(null != topicName && null != message){

        }

    }

    /**
     * @return
     */
    public static String getDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss zzz");
        return sdf.format(new Date());
    }

    public static void stop() {
        if (null != producer) {
            producer.close();
            producer = null;
        }
    }
}
