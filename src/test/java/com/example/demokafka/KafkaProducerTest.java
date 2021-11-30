package com.example.demokafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerTest {

    public static final String TOPIC_NAME = "demo-kafka-topic";

    public static Properties getProperties() {
        Properties props = new Properties();

//        props.put("bootstrap.servers", "47.100.86.147:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.100.86.147:9092,47.100.86.147:9093,47.100.86.147:9094");


        /**
         * 当producer向leader发送数据时，可以通过request.required.acks参数来设置数据可靠性的级别,分别是0, 1，all。
         */
//        props.put("acks", "all");
        props.put(ProducerConfig.ACKS_CONFIG, "all");


        /**
         *请求失败，生产者会自动重试，指定是0次，如果启用重试，则会有重复消息的可能性
         */
//        props.put("retries", 0);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);


        /**
         * 生产者缓存每个分区未发送的消息,缓存的大小是通过 batch.size 配置指定的，默认值是16KB
         */
//        props.put("batch.size", 16384);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);


        /**
         * 默认值就是0，消息是立刻发送的，即便batch.size缓冲空间还没有满
         * 如果想减少请求的数量，可以设置 linger.ms 大于0，即消息在缓冲区保留的时间，超过设置的值就会被提交到          服务端
         * 通俗解释是，本该早就发出去的消息被迫至少等待了linger.ms时间，相对于这时间内积累了更多消息，批量发送           减少请求
         * 如果batch被填满或者linger.ms达到上限，满足其中一个就会被发送
         */
//        props.put("linger.ms", 1);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        /**
         * buffer.memory的用来约束Kafka Producer能够使用的内存缓冲的大小的，默认值32MB。
         * 如果buffer.memory设置的太小，可能导致消息快速的写入内存缓冲里，但Sender线程来不及把消息发送到             Kafka服务器
         * 会造成内存缓冲很快就被写满，而一旦被写满，就会阻塞用户线程，不让继续往Kafka写消息了
         * buffer.memory要大于batch.size，否则会报申请内存不#足的错误，不要超过物理内存，根据实际情况调整
         * 需要结合实际业务情况压测进行配置
         */
//        props.put("buffer.memory", 33554432);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);


        /**
         * key的序列化器，将用户提供的 key和value对象ProducerRecord 进行序列化处理，key.serializer必须被          设置，
         * 即使消息中没有指定key，序列化器必须是一个实
         org.apache.kafka.common.serialization.Serializer接口的类，
         * 将key序列化成字节数组。
         */
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }


    /**
     * send()方法是异步的，添加消息到缓冲区等待发送，并立即返回
     * 生产者将单个的消息批量在一起发送来提高效率,即 batch.size和linger.ms结合
     * <p>
     * 实现同步发送：一条消息发送之后，会阻塞当前线程，直至返回 ack
     * 发送消息后返回的一个 Future 对象，调用get即可
     * <p>
     * 消息发送主要是两个线程：一个是Main用户主线程，一个是Sender线程
     * 1)main线程发送消息到RecordAccumulator即返回
     * 2)sender线程从RecordAccumulator拉取信息发送到broker
     * 3) batch.size和linger.ms两个参数可以影响 sender 线程发送次数
     */

    @Test
    public void producerSendTest() {
        Properties properties = getProperties();
        Producer<String, String> producer = new KafkaProducer<>(properties);
        System.out.println();
        for (int i = 0; i < 3; i++) {
            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>(TOPIC_NAME, "demo-key" + i, "demo-value" + i));
            try {
                RecordMetadata recordMetadata = future.get();
                System.out.println("发送状态：" + recordMetadata.toString());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    @Test
    public void ProducerSendWithCallbackTest() {
        Properties properties = getProperties();
        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 3; i++) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "demo-key" + i, "demo-value" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.err.println("发送状态：" + metadata.toString());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }

    /**
     * 发送消息携带回调函数，指定某个分区
     * 实现顺序消息
     */
    @Test
    public void SendWithCallbackAndPartitionTest() {
        Properties properties = getProperties();
        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, 4, "demo-key" + i, "demo-value" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.err.println("发送状态：" + metadata.toString());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }

    /**
     * 自定义分区策略
     */
    @Test
    public void SendWithCallbackAndCustomPartitionTest() {
        Properties properties = getProperties();
        properties.put("partitioner.class", "com.example.demokafka.config.CustomPartitioner");
        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "demo-kafka", "demo-value" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.err.println("发送状态：" + metadata.toString());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }
}







