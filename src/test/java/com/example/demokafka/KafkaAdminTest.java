package com.example.demokafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminTest {

    private static final String TOPIC_NAME = "user.register.topic";

    /**
     * 设置admin 客户端
     *
     * @return
     */
    public static AdminClient initAdminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            "47.101.187.117:9092,47.101.187.117:9093,47.101.187.117:9094");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /**
     * 创建topic
     */
    @Test
    public void createTopicTest() {
        AdminClient adminClient = initAdminClient();
        //指定分区数量，副本数量
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 6, (short) 3);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(List.of(newTopic));
        try {
            //future等待创建，成功则不会有任何报错
            createTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看topic 列表
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void listTopicTest() throws ExecutionException, InterruptedException {
        AdminClient adminClient = initAdminClient();
        //是否查看内部的topic，可以不用
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);

        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> topics = listTopicsResult.names().get();
        for (String name : topics) {
            System.err.println(name);
        }
    }

    /**
     * 删除topic
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void delTopicTest() throws ExecutionException, InterruptedException {
        AdminClient adminClient = initAdminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        deleteTopicsResult.all().get();
    }

    /**
     * 查看topic详情
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void detailTopicTest() throws ExecutionException, InterruptedException {
        AdminClient adminClient = initAdminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();

        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        entries.stream().forEach((entry) -> System.out.println("name:" + entry.getKey() + ",desc:" + entry.getValue()));
    }

    /**
     * 增加Topic分区数量
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void incrPartitionTopicTest() throws ExecutionException, InterruptedException {
        Map<String, NewPartitions> infoMap = new HashMap<>(1);
        AdminClient adminClient = initAdminClient();
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        infoMap.put(TOPIC_NAME, newPartitions);
        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(infoMap);
        createPartitionsResult.all().get();
    }

//    DefaultPartitioner

}







