package ccjz.rgzn.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.mina.filter.executor.IoEventQueueHandler;
import org.eclipse.jetty.util.thread.strategy.ProduceConsume;

import java.util.Properties;

public class ProduceDemo {
    private static final String SERVER = "node1:9092,node2:9092,node3:9092";
    public static void main(String[] args) throws IDException {
        /**
         * 步骤1,配置生产参数
         * @param args
         */
        Properties props = new Properties();
        //配置方式1.
        //props.load(Producer.class.getClosslooder().getClassLooder().getResourceAsStream("ckient.properties);
        //props.put(bootstarp.servers)

        //配置方式2. 利用常量类，去进行配置，不容易写错参数名，比较容易记忆
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10000);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, String.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, String.class.getName());

        //关闭kafka幂等性的功能
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

        /**
         *步骤2.创建相应的生产者实例
         */
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        /**
         * 步骤3.构建待发送的消息
         */
        for (int i = 0; i<=10;i++)
        producerRecord<String,String> msg  = new Producercord<String, String>(topic:"tpc_1",key:"name"+1,value:"bigdata1902"+RandmoStringUtils.radomAlphaletic(count:4,7));
        /**
         * 步骤3.构建待发送的消息
         */
        producer.send(smg);
        /**
         * 步骤4.关闭生产者实例
         */

    }

    private static class IDException extends Exception {
    }
}