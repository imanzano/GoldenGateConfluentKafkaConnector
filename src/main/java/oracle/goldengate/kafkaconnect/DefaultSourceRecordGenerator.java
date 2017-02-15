/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect;

import java.util.Date;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.adapt.Tx;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the default implementation of the the SourceRecordGenerator.
 * The topic is the source table short name.
 * The key is the source position.
 * Explicit partition selection functionality is not provided.  If it is not
 * provided then partition is selected based on the hash code of the key.
 * In the default implementation the key is the position the 
 * partition selection should be random.  Position is the concatenation of the
 * trail file sequence number plus the trail file offset (RBA).
 * @author tbcampbe
 */
public class DefaultSourceRecordGenerator implements SourceRecordGenerator{
    final private Logger logger = LoggerFactory.getLogger(DefaultSourceRecordGenerator.class);

    @Override
    public SourceRecord createSourceRecord(Tx transaction, 
            Op operation, 
            KafkaProducer<?,?> producer,
            Struct key,
            Struct payload) {
        //The topic name will default to the source fully qualified table name.
        //So schema.table for two part names and
        //catalog.schema.table for three part names.
        final String topic = operation.getTableName().getOriginalName();

        // The partition used will be the position.  
        // The position is a concatenation of the sequence file number
        // of the source trail file + the offset of the operation 
        // from the source trail file.  
        // This logic should result in random partition selection.
        final String pos = operation.getPosition();

        // Alternatively, we could partition by tablename
        // String key = operation.getTableName.getOriginalName();
        // partition = Collections.singletonMap("table", topic);
        
        // We'll use the operation date as offset (assuming
        // that they will be ordered).   If we partition by name,
        // we could use the operation's position as the offset.
        final Map<String, Long> offset = new HashMap<>();
        //This is probably not a good timestamp to use, plus it fails
        //Date opDate = operation.getOperation().getTimestamp().getDate();
        final Date opDate = new Date();
        offset.put("timestamp", opDate.getTime());

        //SourceRecord sr = 
        //        new SourceRecord(partition, offset, topic, record.schema(), record);
        final SourceRecord sr;
        final Map<String, String> partition = Collections.singletonMap("position", pos);
        if (key != null){
            sr = new SourceRecord(partition, offset, topic, null, key.schema(), key, payload.schema(), payload );
        }else{
            sr = new SourceRecord(partition, offset, topic, payload.schema(), payload);
            
        }
        return sr;
    }
    
}
