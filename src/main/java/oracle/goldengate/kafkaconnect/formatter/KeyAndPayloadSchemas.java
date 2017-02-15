/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect.formatter;

import org.apache.kafka.connect.data.Schema;

/**
 * A storage class for the key and payload schemas.
 * @author tbcampbe
 */
public class KeyAndPayloadSchemas {
    
    Schema keySchema;
    Schema payloadSchema;
    
    /**
     * Method to set the key schema.
     * @param s The key schema
     */
    public void setKeySchema(Schema s){
        keySchema = s;
    }
    
    /**
     * Method to get the key schema.
     * @return The key schema.
     */
    public Schema getKeySchema(){
        return keySchema;
    }
    
    /**
     * Method to set the payload schema.
     * @param s The payload schema.
     */
    public void setPayloadSchema(Schema s){
        payloadSchema = s;
    }
    
    /**
     * Method to get the payload schema.
     * @return The payload schema.
     */
    public Schema getPayloadSchema(){
        return payloadSchema;
    }
}
