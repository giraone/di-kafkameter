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
package com.di.jmeter.kafka.config;

import com.di.jmeter.kafka.utils.VariableSettings;
import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testbeans.TestBeanHelper;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import static org.apache.jmeter.threads.JMeterContextService.getContext;

public class KafkaConsumerConfig extends ConfigTestElement
        implements ConfigElement, TestBean, TestStateListener, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    private static final long serialVersionUID = 3328926106250797599L;

    private List<VariableSettings> extraConfigs;
    private String kafkaConsumerClientVariableName;
    private String kafkaBrokers;
    private String groupId;
    private boolean autoCommit;
    private String deSerializerKey;
    private String deSerializerValue;
    private String securityType;
    private String kafkaSslKeystore; // Kafka ssl keystore (include path information); e.g; "server.keystore.jks"
    private String kafkaSslKeystorePassword; // Keystore Password
    private String kafkaSslTruststore;
    private String kafkaSslTruststorePassword;
    private String kafkaSslPrivateKeyPass;

    @Override
    public void addConfigElement(ConfigElement config) {

    }
    @Override
    public boolean expectsModification() {
        return false;
    }

    @Override
    public void testStarted() {
        this.setRunningVersion(true);
        TestBeanHelper.prepare(this);
        final JMeterVariables variables = getContext().getVariables();
        final String variableNameProperties = getKafkaConsumerClientVariableName();
        final Properties consumerProperties = (Properties) variables.getObject(variableNameProperties);
        if (consumerProperties != null) {
            LOGGER.error("Kafka consumer is already running.");
        } else {
            variables.putObject(variableNameProperties, getProps());
            LOGGER.error("Kafka consumer properties added to '{}'.", variableNameProperties);
        }
    }

    private Properties getProps() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, getGroupId());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getDeSerializerKey());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getDeSerializerValue());
        props.put("security.protocol", getSecurityType().replaceAll("securityType.", "").toUpperCase());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, isAutoCommit());

        LOGGER.debug("Adding {} additional configs", getExtraConfigs().size());
        for (VariableSettings entry : getExtraConfigs()){
            props.put(entry.getConfigKey(), entry.getConfigValue());
            LOGGER.debug("Adding property: {}", entry.getConfigKey());
        }

        // Use the StickyAssignor to avoid re-assignment
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        // Poll only 0 or 1 records, so that JMeter Summary Report counts correctly
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        if (getSecurityType().equalsIgnoreCase("securityType.ssl") || getSecurityType().equalsIgnoreCase("securityType.sasl_ssl")) {
            LOGGER.info("Kafka security type: {}", getSecurityType().replaceAll("securityType.", "").toUpperCase());
            LOGGER.info("Setting up Kafka {} properties", getSecurityType());
            props.put("ssl.truststore.location", getKafkaSslTruststore());
            props.put("ssl.truststore.password", getKafkaSslTruststorePassword());
            props.put("ssl.keystore.location", getKafkaSslKeystore());
            props.put("ssl.keystore.password", getKafkaSslKeystorePassword());
            props.put("ssl.key.password", getKafkaSslPrivateKeyPass());
        }
        return props;
    }

    @Override
    public void testStarted(String host) {
        testStarted();
    }

    @Override
    public void testEnded() {
    }

    @Override
    public void testEnded(String host) {
        testEnded();
    }

    // Getters and setters

    public String getKafkaConsumerClientVariableName() {
        return kafkaConsumerClientVariableName;
    }

    @SuppressWarnings("unused")
    public void setKafkaConsumerClientVariableName(String kafkaConsumerClientVariableName) {
        this.kafkaConsumerClientVariableName = kafkaConsumerClientVariableName;
    }

    public String getKafkaBrokers() {
        return kafkaBrokers;
    }

    @SuppressWarnings("unused")
    public void setKafkaBrokers(String kafkaBrokers) {
        this.kafkaBrokers = kafkaBrokers;
    }

    public String getSecurityType() {
        return securityType;
    }

    @SuppressWarnings("unused")
    public void setSecurityType(String securityType) {
        this.securityType = securityType;
    }

    public String getKafkaSslKeystore() {
        return kafkaSslKeystore;
    }

    @SuppressWarnings("unused")
    public void setKafkaSslKeystore(String kafkaSslKeystore) {
        this.kafkaSslKeystore = kafkaSslKeystore;
    }

    public String getKafkaSslKeystorePassword() {
        return kafkaSslKeystorePassword;
    }

    @SuppressWarnings("unused")
    public void setKafkaSslKeystorePassword(String kafkaSslKeystorePassword) {
        this.kafkaSslKeystorePassword = kafkaSslKeystorePassword;
    }

    public String getKafkaSslTruststore() {
        return kafkaSslTruststore;
    }

    @SuppressWarnings("unused")
    public void setKafkaSslTruststore(String kafkaSslTruststore) {
        this.kafkaSslTruststore = kafkaSslTruststore;
    }

    public String getKafkaSslTruststorePassword() {
        return kafkaSslTruststorePassword;
    }

    @SuppressWarnings("unused")
    public void setKafkaSslTruststorePassword(String kafkaSslTruststorePassword) {
        this.kafkaSslTruststorePassword = kafkaSslTruststorePassword;
    }

    public String getKafkaSslPrivateKeyPass() {
        return kafkaSslPrivateKeyPass;
    }

    @SuppressWarnings("unused")
    public void setKafkaSslPrivateKeyPass(String kafkaSslPrivateKeyPass) {
        this.kafkaSslPrivateKeyPass = kafkaSslPrivateKeyPass;
    }

    @SuppressWarnings("unused")
    public void setExtraConfigs(List<VariableSettings> extraConfigs) {
        this.extraConfigs = extraConfigs;
    }

    public List<VariableSettings> getExtraConfigs() {
        return this.extraConfigs;
    }

    public String getGroupId() {
        return groupId;
    }

    @SuppressWarnings("unused")
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    @SuppressWarnings("unused")
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public String getDeSerializerKey() {
        return deSerializerKey;
    }

    @SuppressWarnings("unused")
    public void setDeSerializerKey(String deSerializerKey) {
        this.deSerializerKey = deSerializerKey;
    }

    public String getDeSerializerValue() {
        return deSerializerValue;
    }

    @SuppressWarnings("unused")
    public void setDeSerializerValue(String deSerializerValue) {
        this.deSerializerValue = deSerializerValue;
    }
}
