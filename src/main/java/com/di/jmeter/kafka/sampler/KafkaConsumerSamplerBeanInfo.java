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

import org.apache.jmeter.testbeans.BeanInfoSupport;

import java.beans.PropertyDescriptor;

public class KafkaConsumerSamplerBeanInfo extends BeanInfoSupport {

    public KafkaConsumerSamplerBeanInfo() {
        super(KafkaConsumerSampler.class);
        createPropertyGroup("Test Settings", new String[] {
            "kafkaConsumerClientVariableName",
            "commaSeparatedTopicNames",
            "closeConsumerAtTestEnd"
        });
        createPropertyGroup("Consumer Settings", new String[] {
            "pollTimeout",
            "commitType"
        });

        PropertyDescriptor varPropDesc = property("kafkaConsumerClientVariableName");
        varPropDesc.setValue(NOT_UNDEFINED, Boolean.TRUE);
        varPropDesc.setValue(DEFAULT, "KafkaConsumerClient");
        varPropDesc.setDisplayName("Name of Consumer Client");
        varPropDesc.setShortDescription("Variable name declared in Kafka Consumer client config");

        PropertyDescriptor topicsPropDesc = property("commaSeparatedTopicNames");
        topicsPropDesc.setValue(NOT_UNDEFINED, Boolean.TRUE);
        topicsPropDesc.setValue(DEFAULT, "kafka_topic");
        topicsPropDesc.setDisplayName("Topics to consume from");
        topicsPropDesc.setShortDescription("Comma separated names of Kafka topics for the consumer to subscribe");

        PropertyDescriptor propDesc = property("closeConsumerAtTestEnd");
        propDesc.setValue(NOT_UNDEFINED, Boolean.TRUE);
        propDesc.setValue(DEFAULT, Boolean.TRUE);
        propDesc.setDisplayName("Close Consumer at each test end");
        propDesc.setShortDescription("Close Consumer at each end of the test");

        PropertyDescriptor consumerSettingsPropDesc = property("pollTimeout");
        consumerSettingsPropDesc.setValue(NOT_UNDEFINED, Boolean.TRUE);
        consumerSettingsPropDesc.setValue(DEFAULT, "100");
        consumerSettingsPropDesc.setDisplayName("Poll Timeout");
        consumerSettingsPropDesc.setShortDescription("Consumer poll timeout (in milliseconds)");

        consumerSettingsPropDesc = property("commitType");
        consumerSettingsPropDesc.setValue(NOT_UNDEFINED, Boolean.TRUE);
        consumerSettingsPropDesc.setValue(DEFAULT, "Sync");
        consumerSettingsPropDesc.setDisplayName("Commit Type");
        consumerSettingsPropDesc.setShortDescription("Commit type - Sync/Async");
    }
}
