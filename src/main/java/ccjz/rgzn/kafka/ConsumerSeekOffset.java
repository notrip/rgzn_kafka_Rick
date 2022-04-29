package ccjz.rgzn.kafka;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.hamcrest.Condition;

import java.time.Duration;
import java.util.*;


public class ConsumerSeekOffset {
    public static <set> void main(String[] arg){
        //1.参数配置
        Properties props = new Properties();
        //key的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //value的反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");
        //设置自动读取的起始offset（偏移量），值可以是：earliest，latest，none
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //设置自动提交offset（偏移量）
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        //设置消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        //2.创建consumer实例对象
        KafkaConsumer<String , String> kafkaConsumer = new KafkaConsumer<String, String>(props);

        //3定语主题

        kafkaConsumer.subscribe(Arrays.asList("tpc_1"));
//        kafkaConsumer.subscribe(Arrays.asList("toc_1"), new ConsumerRebalanceListener() {
//            //在均衡开始前和消费者停止读取消息之后，被调用
//            @Override
//            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
//                //储存，吧没有提交的offset储存到数据库中
//            }
//
//            //再重新被分配到分区和消费者开始读取消息之前，被调用
//            @Override
//            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
//                //拉取
//
//            }
//        });

        //先拉取一次
        kafkaConsumer.poll(Duration.ofMillis(100));

        //先看看被分配了那些topic中的分区
        Set<TopicPartition> assignment = kafkaConsumer.assignment();

        //对于被分配的分区，全部统一定位到offset=100的位置成为初始偏移量
        for (TopicPartition topicPartion : assignment){
            kafkaConsumer.seek(topicPartion,100);           //设置特定偏移量
        }

        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> rec : records){
                //做一些业务处理
                System.out.println(rec.key()+","
                        +rec.value()+","
                        +rec.topic()+","
                        +rec.partition()+","
                        +rec.offset());
                System.out.println("---------------------------------------------------");

            }
        }
    }


}
