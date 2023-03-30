package com.khany.kafka.clip2;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@SpringBootApplication
public class Clip2Application {

    public static void main(String[] args) {
        SpringApplication.run(Clip2Application.class, args);
    }

    @Bean
    public ApplicationRunner runner(AdminClient adminClient) {
        return args -> {
            Map<String, TopicListing> topics = adminClient.listTopics().namesToListings().get();
            for (String topicName : topics.keySet()) {
                TopicListing topicListing = topics.get(topicName);
                System.out.println(topicListing);

                Map<String, TopicDescription> descriptionMap = adminClient.describeTopics(Collections.singleton(topicName)).allTopicNames().get();
                System.out.println(descriptionMap);

                if(!topicListing.isInternal()) {
                    adminClient.deleteTopics(Collections.singleton(topicName));
                }
            }
        };
    }
}
