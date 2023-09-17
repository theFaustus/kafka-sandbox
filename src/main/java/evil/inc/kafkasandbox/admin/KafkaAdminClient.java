package evil.inc.kafkasandbox.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ElectionNotNeededException;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class KafkaAdminClient implements AutoCloseable {

    private final AdminClient admin;

    public KafkaAdminClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        admin = AdminClient.create(properties);
    }

    public List<TopicPartitionInfo> addPartitions(String topic, int numberOfPartitionsToAdd) throws ExecutionException, InterruptedException {
        int existingNumberOfPartitions = describeTopic(topic).partitions().size();
        admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(existingNumberOfPartitions + numberOfPartitionsToAdd)));
        return describeTopic(topic).partitions();
    }

    public String getClusterId() throws ExecutionException, InterruptedException {
        return admin.describeCluster().clusterId().get();
    }

    public Node getClusterController() throws ExecutionException, InterruptedException {
        return admin.describeCluster().controller().get();
    }

    public Collection<Node> getNodes() throws ExecutionException, InterruptedException {
        return admin.describeCluster().nodes().get();
    }

    public void resetOffsetsToEarliest(String groupId) throws InterruptedException, ExecutionException {
        Map<TopicPartition, OffsetAndMetadata> resetOffsets = listConsumerGroupEarliestOffsets(groupId).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue().offset())));
        try {
            admin.alterConsumerGroupOffsets(groupId, resetOffsets).all().get();
        } catch (Exception e) {
            log.error("Failed to update the offsets committed by group {}", groupId, e);
        }
    }

    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listConsumerGroupLatestOffsets(String groupId) throws InterruptedException, ExecutionException {
        Map<TopicPartition, OffsetAndMetadata> offsets = listConsumerGroupOffsets(groupId);
        return admin.listOffsets(offsets.keySet()
                .stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()))).all().get();
    }

    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listConsumerGroupEarliestOffsets(String groupId) throws InterruptedException, ExecutionException {
        Map<TopicPartition, OffsetAndMetadata> offsets = listConsumerGroupOffsets(groupId);
        return admin.listOffsets(offsets.keySet()
                .stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()))).all().get();
    }

    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listConsumerGroupOffsetsEarlierThan(String groupId, ZonedDateTime dateTime) throws InterruptedException, ExecutionException {
        Map<TopicPartition, OffsetAndMetadata> offsets = listConsumerGroupOffsets(groupId);
        return admin.listOffsets(offsets.keySet()
                .stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.forTimestamp(dateTime.toEpochSecond())))).all().get();
    }

    public void deleteRecordsBefore(String groupId, ZonedDateTime dateTime) throws ExecutionException, InterruptedException {
        Map<TopicPartition, RecordsToDelete> recordsToDelete = listConsumerGroupOffsetsEarlierThan(groupId, dateTime).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> RecordsToDelete.beforeOffset(e.getValue().offset())));
        admin.deleteRecords(recordsToDelete).all().get();
    }

    public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String groupId) throws InterruptedException, ExecutionException {
        return admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
    }

    public ConsumerGroupDescription describeConsumerGroup(String consumerGroup) throws InterruptedException, ExecutionException {
        Collection<ConsumerGroupListing> consumerGroupListings = listConsumerGroups();
        return admin.describeConsumerGroups(consumerGroupListings.stream().map(ConsumerGroupListing::groupId).toList())
                .describedGroups()
                .get(consumerGroup).get();
    }

    public Collection<ConsumerGroupListing> listConsumerGroups() throws InterruptedException, ExecutionException {
//        admin.listConsumerGroups().valid().get().forEach(group -> log.info("Retrieved valid consumer group {}", group)); //consumer groups that the cluster returned without errors,
//        admin.listConsumerGroups().errors().get().forEach(group -> log.info("Retrieved error consumer group {}", group)); //get all the exceptions
        return admin.listConsumerGroups().all().get(); //only the first error the cluster returned will be thrown as an exception.
    }

    public void deleteTopic(String topic) {
        try {
            log.info("Deleting topic " + topic);
            admin.deleteTopics(List.of(topic)).all().get();
            describeTopic("CustomersNewTopic");
        } catch (Exception e) {
            log.info("Topic is deleted.");
        }
    }

    public TopicDescription describeOrCreateTopic(String topic, int numberOfPartitions, short replicationFactor) throws InterruptedException, ExecutionException {
        try {
            DescribeTopicsResult topics = admin.describeTopics(List.of(topic));
            return topics.topicNameValues().get(topic).get();
        } catch (Exception e) {
            log.info("Topic {} does not exist. Going to create it now", topic);
            CreateTopicsResult newTopic = admin.createTopics(Collections.singletonList(new NewTopic(topic, numberOfPartitions, replicationFactor)));
            if (newTopic.numPartitions(topic).get() != numberOfPartitions) {
                log.info("Topic has wrong number of partitions.");
                return null;
            } else {
                DescribeTopicsResult topics = admin.describeTopics(List.of(topic));
                return topics.topicNameValues().get(topic).get();
            }
        }
    }

    public TopicDescription describeTopic(String topic) throws ExecutionException, InterruptedException {
        try {
            DescribeTopicsResult topics = admin.describeTopics(List.of(topic));
            return topics.topicNameValues().get(topic).get();
        } catch (Exception e) {
            throw e;
        }
    }

    public void describeTopicAsync(String topic) throws ExecutionException, InterruptedException {
        try {
            DescribeTopicsResult topics = admin.describeTopics(List.of(topic));
            topics.topicNameValues().get(topic).whenComplete((description, throwable) -> {
                if (throwable != null) {
                    log.error("Error trying to describe topic {} ", topic, throwable);
                } else {
                    log.info("Description of topic:" + description);
                }
            });
        } catch (Exception e) {
            throw e;
        }
    }

    private void electLeader(ElectionType electionType, String topic) {
        try {
            admin.electLeaders(electionType, Set.of(new TopicPartition(topic, 0))).all().get();
        } catch (Exception e) {
            if (e.getCause() instanceof ElectionNotNeededException)
                log.info("All leaders are preferred already");
            else log.error("Unexpected error", e);
        }
    }

    public Set<String> listTopics() throws InterruptedException, ExecutionException {
        return admin.listTopics().names().get();
    }

    @Override
    public void close() throws Exception {
        admin.close(Duration.ofSeconds(3));
    }

    public static void main(String[] args) {

        try (KafkaAdminClient admin = new KafkaAdminClient()) {
            String customersNewTopic = "CustomersFoo";
            String consumerGroup = "KafkaConsumerAvro-4";
            String topic = "CustomerCountry";
            admin.listTopics().forEach(t -> log.info("Retrieved topic {}", t));
            log.info("Describing consumerGroup: {}", admin.describeOrCreateTopic(customersNewTopic, 1, (short) 1));
            admin.deleteTopic(customersNewTopic);
            admin.listConsumerGroups().forEach(group -> log.info("Retrieved consumer consumerGroup {}", group));
            log.info("Describing consumer consumerGroup {}", admin.describeConsumerGroup(consumerGroup));

            admin.resetOffsetsToEarliest(consumerGroup);

            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(consumerGroup);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = admin.listConsumerGroupLatestOffsets(consumerGroup);
            offsets.forEach((key, value) -> log.info("Consumer consumerGroup KafkaConsumerAvro-4 has committed offset {} to consumerGroup {} partition {}. " +
                    "The latest offset in the partition is {} so consumer consumerGroup is {} records behind", value.offset(), key.topic(), key.partition(), latestOffsets.get(key).offset(), latestOffsets.get(key).offset() - value.offset()));

            log.info("Cluster info id={}, controller={}, nodes={}", admin.getClusterId(), admin.getClusterController(), admin.getNodes());

            List<TopicPartitionInfo> topicPartitionInfos = admin.addPartitions(topic, 2);
            log.info("Number of partitions for consumerGroup {}", topicPartitionInfos.size());

            admin.deleteRecordsBefore(consumerGroup, ZonedDateTime.now());
            log.info("Deleted records for {}", consumerGroup);

            admin.electLeader(ElectionType.PREFERRED, topic);
//            admin.electLeader(ElectionType.UNCLEAN, topic);


        } catch (Exception e) {
            log.error("Unexpected error", e);
        }

    }


}
