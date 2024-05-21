package com.atguigu.kafka.test.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;

public class AdminTopicTest {
    public static void main(String[] args) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // TODO: create admin object
        final Admin admin = Admin.create(configMap);

        String topicName = "test1";
        int partitionCount = 1; // number of topic partitions
        short replicationCount = 1; // number of replications at each kafka broker
        NewTopic topic1 = new NewTopic(topicName, partitionCount, replicationCount);

        String topicName1 = "test2";
        int partitionCount1 = 2; // number of topic partitions
        short replicationCount1 = 2; // number of replications at each kafka broker
        NewTopic topic2 = new NewTopic(topicName1, partitionCount1, replicationCount1);

        String topicName2 = "test3";
        int partitionCount2 = 2; // number of topic partitions
        short replicationCount2 = 2; // number of replications at each kafka broker
        NewTopic topic3 = new NewTopic(topicName2, partitionCount2, replicationCount2);

        String topicName3 = "test6";
        Map<Integer, List<Integer>> replicateMap = new HashMap<>();
        replicateMap.put(0, Arrays.asList(3, 1)); // first number is leader, the rest is follower
        replicateMap.put(1, Arrays.asList(2, 3));
        replicateMap.put(2, Arrays.asList(1, 2));
        NewTopic topic4 = new NewTopic(topicName3, replicateMap);


        // TODO: create topics
        final CreateTopicsResult result = admin.createTopics(Arrays.asList(topic4));

        // TODO: close admin
        admin.close();

    }
}
