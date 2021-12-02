package com.example.demokafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class UserController {

    private static final String TOPIC_NAME = "user.register.topic";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/api/v1/{num}")
    public void sendMessage(@PathVariable("num") String num) {
        kafkaTemplate.send(TOPIC_NAME, "这是一个消息,num=" + num).addCallback(success -> {
            String topic = success.getRecordMetadata().topic();
            int partition = success.getRecordMetadata().partition();
            long offset = success.getRecordMetadata().offset();
            System.out.println("发送成功:topic=" + topic + ", partition=" + partition + ",offset =" + offset);
        }, failure -> {
            System.out.println("发送消息失败：" + failure.getMessage());
        });
    }

    /**
     * 注解方式的事务
     *
     * @param num
     */
    @GetMapping("/api/v1/tran1")
    @Transactional(rollbackFor = RuntimeException.class)
    public void sendMessage2(int num) {
        kafkaTemplate.send(TOPIC_NAME, "这个是事务消息 1 i=" + num);
        if (num == 0) {
            throw new RuntimeException();
        }
        kafkaTemplate.send(TOPIC_NAME, "这个是事务消息 2 i=" + num);
    }


    /**
     * 声明式事务
     *
     * @param num
     */
    @GetMapping("/api/v1/tran2")
    public void sendMessage3(int num) {
        kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback<String, Object, Object>() {
            @Override
            public Object doInOperations(KafkaOperations<String, Object> kafkaOperations) {
                kafkaOperations.send(TOPIC_NAME, "这个是事务消息 1 i=" + num);
                if (num == 0) {
                    throw new RuntimeException();
                }
                kafkaOperations.send(TOPIC_NAME, "这个是事务消息 2 i=" + num);
                return true;
            }
        });
    }

}
