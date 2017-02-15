/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect;


import java.util.Properties;
import oracle.goldengate.util.ConfigException;
import oracle.goldengate.util.Util;

import org.apache.kafka.clients.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class keeps track of all properties set on the Kafka Handler via the adapters
 * properties file and also the producer configuration file that needs to be in 
 * the classpath for successful Kafka handler configuration.
 * 
 * @author tbcampbe
 */
public class GGProperties {
    final private Logger logger = LoggerFactory.getLogger(GGProperties.class);
    /**
     * The Kafka producer configuration file. This file will be looked up as
     * part of the classpath and should be found. If not found, a Config
     * exception will be thrown.
     */
    private Properties ggProducerProps;
    private String prClassName = DefaultSourceRecordGenerator.class.getName();
    private String kafkaProducerConfigFile = "kafka-producer-default.properties";


    /**
     * Method to read the producer config and perform validation on the properties
     * set on the Kafka Adapter.
     */
    public void loadKafkaProperties() {
        ggProducerProps = new Properties();
        try {
            if (logger.isDebugEnabled()){
                logger.debug("Loading the Kafka producer properties from the file [" + kafkaProducerConfigFile + "].");
            }   
            ggProducerProps.load(this.getClass().getResourceAsStream("/" + kafkaProducerConfigFile));
        } catch (Exception ex) {
            logger.error("Failed to load the Kafka producer properties file [" + kafkaProducerConfigFile + "].", ex);
            throw new ConfigException("Failed to load the Kafka producer properties file [" + kafkaProducerConfigFile + "].", ex);
        }
        //Perform validation
        //Check for key.serialier
        if (!ggProducerProps.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)){
            ggProducerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                    "io.confluent.kafka.serializers.KafkaAvroSerializer");
        }
        //Check for value.serializer
        if (!ggProducerProps.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)){
            ggProducerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                    "io.confluent.kafka.serializers.KafkaAvroSerializer");
        }
        //Check for schema.registry.url
        if (!ggProducerProps.containsKey("schema.registry.url")){
            logger.warn("The Kafka properties did not contain a configuration entry for \"schema.registry.url\".  This is required for Confluent Platform integration.");
        }
        
    }
    
    /**
     * Convenience method to output the key value pairs from the Kafka producer
     * properties file to a string builder.  This will be output to the log
     * file to facilitate troubleshooting/debugging.
     * @param sb The string builder object.
     */
    public void printKafkaConnectionProps(StringBuilder sb){
        for(String key : ggProducerProps.stringPropertyNames()){
            sb.append("    key [");
            sb.append(key);
            sb.append("]  value [");
            sb.append(ggProducerProps.getProperty(key));
            sb.append("]");
            sb.append(System.lineSeparator());
        }
    }
   
    /**
     * Returns an existing Kafka Producer implementation if one exists or creates
     * the required one - either blocking or non-blocking
     * 
     * @return A Producer object.
     */
    public GGProducer instantiateConfluentKafkaProducer() {
        GGProducer kImpl = new GGProducer();
        kImpl.init(ggProducerProps);
        return kImpl;
    }

    /**
     * Method instantiates and returns the implementation of the
     * SourceRecordGenerator interface.
     * @return An instance of the SourceRecordGenerator
     */
    public SourceRecordGenerator getSourceRecordGenerator() {
        logger.info("Creating a new Source Record Generator instance of the class: " + prClassName);
        SourceRecordGenerator prg = (SourceRecordGenerator) Util.newInstance(prClassName); 
        return prg;
    }
    
    /**
     * Method to get the name of the Kafka Producer properties file.
     * @return The file name.
     */
    public String getKafkaProducerConfigFile() {
        return kafkaProducerConfigFile;
    }

    /**
     * Method to set the name of the Kafka Producer properties file.
     * @param kafkaProducerConfigFile The file name.
     */
    public void setKafkaProducerConfigFile(String kafkaProducerConfigFile) {
        this.kafkaProducerConfigFile = kafkaProducerConfigFile;
    }

    /**
     * Method to set the source record generator class.
     * @param prClassName The Source Record generator class name
     */
    public void setSourceRecordGeneratorClass(String prClassName) {
        this.prClassName = prClassName;
    }

    /**
     * Method to get the source record generator class.
     * @return The Source record generator class name.
     */
    public String getSourceRecordGeneratorClassName() {
        return this.prClassName;
    }

}

