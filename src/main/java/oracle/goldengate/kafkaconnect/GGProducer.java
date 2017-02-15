/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import oracle.goldengate.datasource.GGDataSource.Status;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class wraps the KafkaProducer
 * @author tbcampbe
 */
public class GGProducer {
    private static final Logger logger=LoggerFactory.getLogger(GGProducer.class);

    //The key converter
    private Converter keyConverter = null;
    //The value converter
    private Converter valueConverter = null;
    //The Kafka Producer
    private KafkaProducer<byte[],byte[]> kafkaProducer = null;
    
    /**
     * Initialize the Kafka Producer
     * @param kafkaProps The Kafka producer properties
     */
    public void init(Properties kafkaProps){
        logger.info("Opening the Kafka connection.");
        //TBC This is the way I got this, but it does not seem correct.
        //The Kafka Producer is being instantiated from the properties from
        //the configured properties file.  This is controlled by the GoldenGate
        //property gg.handler.name.kafkaProducerConfigFile
        //The GGConfig stuff below does a lot of processing but really does little
        //but instantiate the converters.  Could use some clean up.
        //Instantate the Kafka producer
        kafkaProducer = new KafkaProducer<>(kafkaProps);

        final Map<String, String> propsAsMap = new HashMap<>((Map) kafkaProps);
        final GGConfig config = new GGConfig(propsAsMap);
        //Instantiate the key and value converters
        keyConverter = config.getConfiguredInstance(GGConfig.KEY_CONVERTER_CLASS_CONFIG, Converter.class);
        keyConverter.configure(config.originalsWithPrefix("key.converter."), true);
        valueConverter = config.getConfiguredInstance(GGConfig.VALUE_CONVERTER_CLASS_CONFIG, Converter.class);
        valueConverter.configure(config.originalsWithPrefix("value.converter."), false);
    }
    
    public Status send(SourceRecord record){
        Status status = Status.OK;

        final byte[] key = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
        final byte[] value = valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        //Instantiate the Kafka producer record
	    final ProducerRecord<byte[],byte[]> pRecord = new ProducerRecord<>(record.topic(), record.kafkaPartition(), key, value);
        try{
            kafkaProducer.send(pRecord);
        }catch(final Exception e){
            logger.error("An exception occurred sending a message to Kafka.", e);
            status = Status.ABEND;
        }
        return status;
    }
    
    /**
     * Flush the Kafka Connection.  This should be called at transaction (or
     * grouped transaction) commit to ensure write durability.
     * @return Status.OK if success else any other status.
     */
    public Status flush(){
        logger.debug("Flushing the Kafka connection.");
        Status status = Status.OK;
        try{
            if (kafkaProducer != null){
                kafkaProducer.flush();
            }
        }catch(final Exception e){
            logger.error("An exception occurred flushing to Kafka.", e);
            status = Status.ABEND;
        }
        return status;
    }
    
    /**
     * Close the Kafka producer.
     */
    public void close(){
        logger.info("Closing the Kafka connection.");
        if (kafkaProducer != null){
            //The close connection cannot block indefinitely.  Allowing 10 seconds.
            kafkaProducer.close(10, TimeUnit.SECONDS);
            kafkaProducer = null;
        }
    }
    
    /**
     * Get the Kafka Producer object.  Breaking encapsulation but it needs to 
     * be passed to the SourceRecordGenerator.  Custom code may need to 
     * interrogate the KafkaProducer object to make decisions.
     * @return The KafkaProducer object.
     */
    public KafkaProducer<byte[],byte[]> getKafkaProducer(){
        return kafkaProducer;
    }
}
