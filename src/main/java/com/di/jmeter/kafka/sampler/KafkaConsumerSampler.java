/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.di.jmeter.kafka.sampler;

import com.google.common.base.Strings;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.engine.util.ConfigMergabilityIndicator;
import org.apache.jmeter.gui.Searchable;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.samplers.Sampler;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerSampler extends AbstractTestElement
    implements Sampler, TestBean, ConfigMergabilityIndicator, TestStateListener, TestElement, Serializable, Searchable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerSampler.class);
    private static final long DEFAULT_TIMEOUT = 100;

    private String kafkaConsumerClientVariableName;
    private String commaSeparatedTopicNames;
    private String pollTimeout;
    private String commitType;
    private boolean closeConsumerAtTestEnd;

    private final Map<Integer, KafkaConsumer<String, Object>> consumers = new HashMap<>();

    @Override
    public SampleResult sample(Entry entry) {

        SampleResult result = new SampleResult();
        boolean started = false;
        try {
            result.setSampleLabel(getName());
            result.setDataType(SampleResult.TEXT);
            result.setContentType("application/json");
            result.setDataEncoding(StandardCharsets.UTF_8.name());
            result.setRequestHeaders(String.format("TimeStamp: %s\n", LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))));
            result.sampleStart();
            started = true;
            this.processRecordsToResults(getConsumerRecord(), result);
        } catch (KafkaException e) {
            LOGGER.error("Kafka Consumer config not initialized properly. Check the config element.", e);
            return handleException(result, e);
        } finally {
            if (started) {
                // Must be checked - otherwise "setEndTime must be called after setStartTime" occurs
                result.sampleEnd();
            }
        }
        return result;
    }

    private ConsumerRecord<String, Object> getConsumerRecord() {
        ConsumerRecords<String, Object> records;
        this.pollTimeout = (Strings.isNullOrEmpty(pollTimeout)) ? String.valueOf(DEFAULT_TIMEOUT) : pollTimeout;

        // This will poll one or zero records
        do {
            final Duration timeout = Duration.ofMillis(Long.parseLong(getPollTimeout()));
            LOGGER.debug("KafkaConsumer.poll with timeout {}", timeout);
            records = getKafkaConsumerOfThread().poll(timeout);
        } while (records.isEmpty());

        LOGGER.info("KafkaConsumer.poll returned {} records", records.count());

        ConsumerRecord<String, Object> record = records.iterator().next();
        LOGGER.debug(String.format("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
        // commit offset of the message
        Map<TopicPartition, OffsetAndMetadata> offset = Collections.singletonMap(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)
        );
        if (getCommitType().equalsIgnoreCase("sync")) {
            getKafkaConsumerOfThread().commitSync(offset); //Commit the offset after reading single message
        } else {
            getKafkaConsumerOfThread().commitAsync((OffsetCommitCallback) offset);//Commit the offset after reading single message
        }

        return record;
    }

    private void processRecordsToResults(ConsumerRecord<String, Object> record, SampleResult result) {
        if (record != null) {
            StringBuilder headers = new StringBuilder();
            StringBuilder response = new StringBuilder();
            headers
                .append("Key: ").append(record.key()).append("\n")
                .append("Topic: ").append(record.topic()).append("\n")
                .append("Timestamp: ").append(record.timestamp()).append("\n")
                .append("Partition: ").append(record.partition()).append("\n")
                .append("Offset: ").append(record.offset()).append("\n");
            record.headers().forEach(h -> headers.append(h.key()).append(":").append(new String(h.value(), StandardCharsets.UTF_8)).append("\n"));
            response.append(record.value());
            result.setResponseHeaders(headers.toString());
            result.setResponseData(response.toString(), StandardCharsets.UTF_8.name());
            result.setResponseOK();
        } else {
            result.setResponseData("No record retrieved", StandardCharsets.UTF_8.name());
            result.setResponseCode("401");
        }
    }

    private SampleResult handleException(SampleResult result, Exception ex) {
        result.setResponseMessage("Error consuming messages from kafka topic");
        result.setResponseCode("500");
        result.setResponseData(String.format("Error consuming messages from kafka topic : %s", ex).getBytes());
        result.setSuccessful(false);
        return result;
    }

    @Override
    public boolean applies(ConfigTestElement configTestElement) {
        return false;
    }

    @Override
    public void testStarted() {
    }

    @Override
    public void testStarted(String host) {
        testStarted();
    }

    @Override
    public void testEnded() {
        LOGGER.info("{} Kafka consumer - test ended.", this);
        if (closeConsumerAtTestEnd) {
            consumers.forEach((threadNum, consumer) -> {
                consumer.close(Duration.ofSeconds(10));
                LOGGER.info("{} Kafka consumer of thread {} terminated.", this, threadNum);
            });
            consumers.clear();
        }
    }

    @Override
    public void testEnded(String host) {
        testEnded();
    }

    //Getters and setters

    public String getKafkaConsumerClientVariableName() {
        return kafkaConsumerClientVariableName;
    }

    @SuppressWarnings("unused")
    public void setKafkaConsumerClientVariableName(String kafkaConsumerClientVariableName) {
        this.kafkaConsumerClientVariableName = kafkaConsumerClientVariableName;
    }

    public String getCommaSeparatedTopicNames() {
        return commaSeparatedTopicNames;
    }

    @SuppressWarnings("unused")
    public void setCommaSeparatedTopicNames(String commaSeparatedTopicNames) {
        this.commaSeparatedTopicNames = commaSeparatedTopicNames;
    }

    public String getPollTimeout() {
        return (Strings.isNullOrEmpty(pollTimeout)) ? pollTimeout : String.valueOf(DEFAULT_TIMEOUT);
    }

    @SuppressWarnings("unused")
    public void setPollTimeout(String pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public String getCommitType() {
        return commitType;
    }

    @SuppressWarnings("unused")
    public void setCommitType(String commitType) {
        this.commitType = commitType;
    }

    public boolean isCloseConsumerAtTestEnd() {
        return closeConsumerAtTestEnd;
    }

    @SuppressWarnings("unused")
    public void setCloseConsumerAtTestEnd(boolean closeConsumerAtTestEnd) {
        this.closeConsumerAtTestEnd = closeConsumerAtTestEnd;
    }

    @SuppressWarnings("unchecked")
    private KafkaConsumer<String, Object> getKafkaConsumerOfThread() {

        final JMeterVariables variables = getThreadContext().getVariables();
        final JMeterContext threadContext = getThreadContext();
        final int threadNum = threadContext.getThreadNum();
        LOGGER.debug("{} getKafkaConsumerOfThread for thread {}", this, threadNum);
        // the name under which the properties are stored
        final String variableNameProperties = getKafkaConsumerClientVariableName();
        // the name under which the consumer is stored
        final String variableNameConsumer = getKafkaConsumerClientVariableName() + "_" + threadNum;
        KafkaConsumer<String, Object> consumer = (KafkaConsumer<String, Object>) variables.getObject(variableNameConsumer);
        if (consumer == null) {
            Properties consumerProperties = (Properties) variables.getObject(variableNameProperties);
            if (consumerProperties == null) {
                throw new RuntimeException("Consumer properties for '" + variableNameProperties + "' not found!");
            }
            LOGGER.info("Kafka consumer properties ({}) fetched from configuration '{}'.", consumerProperties.size(), variableNameProperties);

            final String topicNames = getCommaSeparatedTopicNames();
            Collection<String> topicList =  Arrays.asList(topicNames.split("\\s*,\\s*"));
            LOGGER.info("Kafka consumer test for topics {} started.", topicList);

            // final String clientId = String.format("jmeter-%d-%03d", ProcessHandle.current().pid(), threadNum);
            final String clientId = String.format("jmeter-%03d", threadNum);
            consumerProperties.put("client.id", clientId);
            try {
                consumer = new KafkaConsumer<>(consumerProperties);
                consumer.subscribe(topicList);
                variables.putObject(variableNameConsumer, consumer);
                consumers.put(threadNum, consumer);
                LOGGER.info("Created KafkaConsumer {} with client id {} for name '{}' subscribed to topics {}",
                    consumer, clientId, variableNameConsumer, topicList);
            } catch (Exception e) {
                LOGGER.error("Cannot initialize KafkaConsumer '{}' for topics {}!", variableNameConsumer, topicList, e);
                throw e;
            }
        }
        return consumer;
    }
}
