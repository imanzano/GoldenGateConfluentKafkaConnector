/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect.formatter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsType;
import static oracle.goldengate.datasource.meta.DsType.GGSubType.GG_SUBTYPE_FIXED_PREC;
import static oracle.goldengate.datasource.meta.DsType.GGSubType.GG_SUBTYPE_FLOAT;
import oracle.goldengate.datasource.meta.TableMetaData;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class generates the Kafka Connect schema and caches the schemas for
 * reuse.
 * @author tbcampbe
 */
public class KafkaConnectSchemaGenerator {
    private static final Logger logger=LoggerFactory.getLogger(KafkaConnectSchemaGenerator.class);
    
    private final Map<String, KeyAndPayloadSchemas> schemaMap = new HashMap<>();
    private boolean treatAllColumnsAsStrings = false;
    
    /**
     * Method to set to treat all columns as strings.
     * @param allColumnsAsStrings 
     */
    public void setTreatAllColumnsAsStrings(boolean allColumnsAsStrings){
        treatAllColumnsAsStrings = allColumnsAsStrings;
    }
    
    /**
     * Method to get the  schema.  If a schema is not available it will be
     * generated.
     * @param tableName The fully qualified table name.
     * @param tmeta The table metadata object.
     * @return An object holding the key and value schemas.
     */
    public KeyAndPayloadSchemas getSchema(String tableName, TableMetaData tmeta){
        KeyAndPayloadSchemas schemas = schemaMap.get(tableName);
        if (schemas == null){
            logger.info("Building the key and payload schemas for source table [" + tableName + "]");
            schemas = new KeyAndPayloadSchemas();
            //Generate the Kafka key schema
            final Schema keySchema = generateKeySchema(tableName, tmeta);
            //Log the key schema if debug logging enabled.
            logSchema(keySchema);
            schemas.setKeySchema(keySchema);
            //Generate the Kafka value schema
            final Schema payloadSchema = generatePayloadSchema(tableName, tmeta);
            //Log the payload schema if debug logging is enabled.
            logSchema(payloadSchema);
            schemas.setPayloadSchema(payloadSchema);
            schemaMap.put(tableName, schemas);
        }
        
        return schemas;
    }
    
    /**
     * Method to drop an already created schema in the event of a metadata change
     * event.
     * @param tableName The fully qualified table name.
     */
    public void dropSchema(String tableName){
        schemaMap.remove(tableName);
    }
    
    private Schema generateKeySchema(String tableName, TableMetaData tmeta){
        logger.info("Generating key schema for table [" + tableName +"].");
        Schema keySchema = null;
        if (tmeta.getNumKeyColumns() < 1){
            logger.info("The source table [" + tableName + "] contains no primary keys.  The key schema will be null.");
        }else{
            logger.info("The source table [" + tableName + "] contains one or more primary keys.");
            SchemaBuilder builder = SchemaBuilder.struct().name(tableName + "_key");
            for (int col = 0; col < tmeta.getNumColumns(); col++) {
                ColumnMetaData cmeta = tmeta.getColumnMetaData(col);
                if (cmeta.isKeyCol()){
                    addFieldSchema(cmeta, builder);
                }
            }
            //Key schema should be done
            keySchema = builder.schema();
            
        }
        //May return null if the source table has no primary key
        return keySchema;
    }
    
    private Schema generatePayloadSchema(String tableName, TableMetaData tmeta) {
        // TODO: Detect changes to metadata, which will require schema updates
        logger.info("Generating payload schema for table [" + tableName + "]");
        final SchemaBuilder builder = SchemaBuilder.struct().name(tableName);

        //Add a field for the table name
        builder.field("table", Schema.STRING_SCHEMA);
        builder.field("op_type", Schema.STRING_SCHEMA);
        builder.field("op_ts", Schema.STRING_SCHEMA);
        builder.field("current_ts", Schema.STRING_SCHEMA);
        builder.field("pos", Schema.STRING_SCHEMA);
        //An array field for primary key column names could be added here
        //A map field for token values from the source trail file could be added here.

        for (int col = 0; col < tmeta.getNumColumns(); col++) {
            final ColumnMetaData cmeta = tmeta.getColumnMetaData(col);
            addFieldSchema(cmeta, builder);
        }
        return builder.build();
    }
    
    private void addFieldSchema(ColumnMetaData cmeta, SchemaBuilder builder){    
        final String fieldName = cmeta.getColumnName();
        
        if (treatAllColumnsAsStrings){
            //Treat it as a string
            builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA); 
        }else{

            final DsType.GGType colType = cmeta.getDataType().getGGDataType();
            //Variables are always optional
            //if (metadata.getColumnMetaData(col).isNullable()) {
                //Per Lego this always returns true.
            //    optional = true;
            //}
            switch (colType) {
                // Things that fit in signed short
                case GG_16BIT_S: 
                case GG_16BIT_U: 
                case GG_32BIT_S: 
                case GG_32BIT_U: 
                case GG_64BIT_S: 
                    if (cmeta.getDataType().getScale() > 0){
                        builder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
                    }else{
                        builder.field(fieldName, Schema.OPTIONAL_INT64_SCHEMA);
                    }
                    break;
                case GG_64BIT_U:
                    builder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
                    break;
                // REAL is a single precision floating point value, i.e. a Java float
                case GG_REAL: 
                case GG_IEEE_REAL:
                    builder.field(fieldName, Schema.OPTIONAL_FLOAT32_SCHEMA);
                    break;
                case GG_DOUBLE: 
                case GG_IEEE_DOUBLE: 
                case GG_DOUBLE_V:
                case GG_DOUBLE_F:
                case GG_DEC_U:
                case GG_DEC_LSS:
                case GG_DEC_LSE:
                case GG_DEC_TSS:
                case GG_DEC_TSE:
                case GG_DEC_PACKED: 
                    builder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
                    break;
                case GG_ASCII_V:
                case GG_ASCII_F:
                    if (cmeta.getDataType().getGGDataSubType() == GG_SUBTYPE_FLOAT ||
                            cmeta.getDataType().getGGDataSubType() == GG_SUBTYPE_FIXED_PREC) {
                        // This is a number data, let's use Double for consistency.
                        builder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
                    } else {
                        builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
                    }
                    break;
                    // Default to strings for everything else
                default: 
                    builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);       
            }
        }
    }
    
    /**
     * A utility method to log the contents of a schema just for debugging.
     * @param s The schema to be logged.
     */
    private void logSchema(Schema s){
        if ((logger.isDebugEnabled()) && (s != null)){
            StringBuilder sb = new StringBuilder();
            sb.append("Kafka Connect Schema [");
            sb.append(s.name());
            sb.append("]");
            sb.append(System.lineSeparator());
            final List<Field> fields = s.fields();
            for (final Field field: fields){
                sb.append("  Field [");
                sb.append(field.name());
                sb.append("] Type [");
                sb.append(field.schema().toString());
                sb.append("]");
                sb.append(System.lineSeparator());
            }
            logger.debug(sb.toString());
        }
    }
    
}
