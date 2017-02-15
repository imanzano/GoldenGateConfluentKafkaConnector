/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect;

import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.adapt.Tx;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * This is the interface for a class to generate the Kafka producer record.
 * The producer record controls the topic and partition selected.  A default
 * implementation is provided but it is foreseeable that customer use cases for
 * topic name selection and partition selection may vary.  This interface gives
 * customers a way to plug in their required functionality.
 * @author tbcampbe
 */
public interface SourceRecordGenerator {
    
    /**
     * The method to create the producer record.
     * @param transaction The GG transaction
     * @param operation The GG current operation.
     * @param producer The Kafka Producer in case this code needs to interrogate
     * partition information.
     * @param key The key struct.
     * @param payload The payload struct.
 
     * @return A Kafka ProducerRecord object
     */
    SourceRecord createSourceRecord(Tx transaction,
            Op operation, 
            KafkaProducer<?,?> producer,
            Struct key,
            Struct payload);
}
