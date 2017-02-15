/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect.formatter;

import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.format.NgFormatter;
import oracle.goldengate.datasource.format.NgUniqueTimestamp;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.format.NgFormattedData;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static oracle.goldengate.datasource.meta.DsType.GGSubType.GG_SUBTYPE_FIXED_PREC;
import static oracle.goldengate.datasource.meta.DsType.GGSubType.GG_SUBTYPE_FLOAT;

/**
 * This formatted formats operations into Kafka Connect row operation and returns the 
 * data as a Kafka Connect Source Record object.
 * @author tbcampbe
 */
public class KafkaConnectFormatter implements NgFormatter {
    private static final Logger logger=LoggerFactory.getLogger(KafkaConnectFormatter.class);
    //The schema generator 
    private KafkaConnectSchemaGenerator schemaGenerator = null;
    
    //The default operation keys 
    private String insertOpKey = "I";		// inserts
    private String updateOpKey = "U";		// updates
    private String deleteOpKey = "D";		// deletes

    //The action to take if the handler encounters a primary key update
    private PkHandling pkHandling = PkHandling.PK_ABEND;
    //Optional functionality to treat all values as strings
    private boolean treatAllColumnsAsStrings = false;
    //Version Avro Schemas
    private final boolean versionAvroSchemas = false;
    //Use ISO8601 format for current timestamp
    private boolean useIso8601Format = true;
    
        /**
     * Method to set the insert operation key.  This key will be included in the
     * output for insert operations.  The default is "I".
     * @param opKey The insert operation key.
     */
    public void setInsertOpKey(String opKey){
        insertOpKey = opKey;
    }
    
    /**
     * Method to set the update operation key.  This key will be included in the
     * output for update operations.  The default is "U".
     * @param opKey The update operation key.
     */
    public void setUpdateOpKey(String opKey){
        updateOpKey = opKey;
    }
    
    /**
     * Method to set the delete operation key.  This key will be included in the
     * output for delete operations.  The default is "D".
     * @param opKey The delete operation key.
     */
    public void setDeleteOpKey(String opKey){
        deleteOpKey = opKey;
    }
    
    /**
     * The default handling of the Kafka Connect formatter is to map column 
     * values from the source definitions the best fit for a corresponding type.  
     * Set this method to true to alternatively treat all columns as strings.
     * This feature is provided in Jackson which is where the idea came from.
     * @param allColumnsAsStrings True to treat all columns as strings.
     */
    public void setTreatAllColumnsAsStrings(boolean allColumnsAsStrings){
        treatAllColumnsAsStrings = allColumnsAsStrings;
    }
    
    /**
     * Method to set if to use the ISO-8601 format for the current data timestamp.
     * @param iso8601 True to use ISO-8601 format, else false.
     */
    public void setIso8601Format(boolean iso8601){
        useIso8601Format = iso8601;
    }
    
    /**
     * Method to set what action to take in the case of a PK update (primary 
     * key update).  The default is to ABEND.  Set as follows:
     * abend - to abend
     * update - to treat as a normal update
     * delete-insert - to tream as a delete operation and then an insert operation.
     * @param handling abend
     */
    public void setPkUpdateHandling(String handling){
        if (PkHandling.PK_ABEND.compareAction(handling)){
            pkHandling = PkHandling.PK_ABEND;
        }else if (PkHandling.PK_UPDATE.compareAction(handling)){
            pkHandling = PkHandling.PK_UPDATE;
        }else if (PkHandling.PK_DELETE_INSERT.compareAction(handling)){
            pkHandling = PkHandling.PK_DELETE_INSERT;
        }else{
            logger.warn("The value in the Kafka Connect Formatter of [" 
                    + handling 
                    + "] for primary key handling is not valid.  "
                    +  "The default behavior is to ABEND");
            pkHandling = PkHandling.PK_ABEND;
        }
    }
    
    @Override
    public NgFormattedData createNgFormattedData() {
        return new KafkaConnectFormattedData();
    }

    @Override
    public void init(DsConfiguration dc, DsMetaData dmd) {
        if (logger.isInfoEnabled()){
            final StringBuilder sb = new StringBuilder();
            sb.append(System.lineSeparator());
            sb.append("**** Begin Kafka Connect Row Formatter - Configuration Summary ****");
            sb.append(System.lineSeparator());

            sb.append("  Operation types are always included in the formatter output.");
            sb.append(System.lineSeparator());
            sb.append("    The key for insert operations is [");
            sb.append(insertOpKey);
            sb.append("].");
            sb.append(System.lineSeparator());
            sb.append("    The key for update operations is [");
            sb.append(updateOpKey);
            sb.append("].");
            sb.append(System.lineSeparator());
            sb.append("    The key for delete operations is [");
            sb.append(deleteOpKey);
            sb.append("].");
            sb.append(System.lineSeparator());
            //How will column type mapping
            if (treatAllColumnsAsStrings){
                sb.append("  Column type mapping has been configured to treat all source columns as strings.");
            }else{
                sb.append("  Column type mapping has been configured to map source column types to an appropriate corresponding Kafka Connect Schema type.");
            }
            sb.append(System.lineSeparator());
            
            //How are primary key updates handled.
            if(pkHandling == PkHandling.PK_ABEND){
                //Need to abend
                sb.append("  In the event of a primary key update, the Formatter will ABEND.");
            }else if(pkHandling == PkHandling.PK_UPDATE){
                sb.append("  In the event of a primary key update, the Formatter will process it as a normal update.");
            }else if(pkHandling == PkHandling.PK_DELETE_INSERT){
                sb.append("  In the event of a primary key update, the Formatter will process it as a delete and an insert.");
            }
            sb.append(System.lineSeparator());
            
            if (useIso8601Format){
                sb.append("  The current timestamp will be in ISO-8601 format.");
            }else{
                sb.append("  The current timestamp will not be in ISO-8601 format.");
            }
            sb.append(System.lineSeparator());
            sb.append("**** End Kafka Connect Row Formatter - Configuration Summary ****");
            sb.append(System.lineSeparator());
            logger.info(sb.toString());
        }
        //Instantiate the schema generator.
        schemaGenerator = new KafkaConnectSchemaGenerator();
        schemaGenerator.setTreatAllColumnsAsStrings(treatAllColumnsAsStrings);
    }

    @Override
    public void beginTx(DsTransaction dt, DsMetaData dmd, NgFormattedData nfd) throws Exception {
        //NOOP
    }

    @Override
    public void formatOp(DsTransaction tx, DsOperation op, TableMetaData tMeta, NgFormattedData output) throws Exception {
		String tableName = tMeta.getTableName().getOriginalName();
        logger.debug("Entering formatOp");
        try{
            final KafkaConnectFormattedData objectFormattedData = (KafkaConnectFormattedData)output;
            final KeyAndPayloadSchemas schemas = schemaGenerator.getSchema(tableName, tMeta);
            
            final Struct rec1 = new Struct(schemas.getPayloadSchema());
            Struct key1 = null;
            if (schemas.getKeySchema() != null){
                key1 = new Struct(schemas.getKeySchema());
            }

            Struct rec2 = null;
            Struct key2 = null;
            if (op.getOperationType().isInsert()){
                //Insert is after values
                formatAfterValuesOp(op.getOperationType(), tx, op, tMeta, rec1, key1);
            }else if (op.getOperationType().isDelete()){
                //Delete is before values
                formatBeforeValuesOp(op.getOperationType(), tx, op, tMeta, rec1, key1);
            }else if (op.getOperationType().isPkUpdate()){
                //Primary key updates are a special case of update and have
                //optional handling.
                if(pkHandling == PkHandling.PK_ABEND){
                    //Need to abend
                    logger.error("The Kafka Connect Formatter encountered a update including a primary key.  The behavior is configured to ABEND in this scenario.");
                    throw new RuntimeException("The Kafka Connect Formatter encountered a update including a primary key.  The behavior is configured to ABEND in this scenario.");
                }else if(pkHandling == PkHandling.PK_UPDATE){
                    formatAfterValuesOp(DsOperation.OpType.DO_UPDATE, tx, op, tMeta, rec1, key1);
                }else if(pkHandling == PkHandling.PK_DELETE_INSERT){
                    formatBeforeValuesOp(DsOperation.OpType.DO_DELETE, tx, op, tMeta, rec1, key1);
                    rec2 = new Struct(schemas.getPayloadSchema());
                    if (schemas.getKeySchema() != null){
                        key2 = new Struct(schemas.getKeySchema());
                    }
                    formatAfterValuesOp(DsOperation.OpType.DO_INSERT, tx, op, tMeta, rec2, key2);
                }
            }else if (op.getOperationType().isUpdate()){
                //Update is after values
                formatAfterValuesOp(op.getOperationType(), tx, op, tMeta, rec1, key1);
            }else{
                //Unknown operation, log a warning and move on.
                logger.error("The Formatter encounter an unknown operation ["
                    + op.getOperationType() + "].");
                throw new RuntimeException("The Formatter encounter an unknown operation ["
                    + op.getOperationType() + "].");
            }
            //Set the objects
            objectFormattedData.setRecord(rec1);
            objectFormattedData.setKey(key1);
            //Only will be a second object if this is a primary key update
            objectFormattedData.setRecord(rec2);
            objectFormattedData.setKey(key2);
  
        }catch(final Exception e){
            logger.error("The Kafka Connect Row Formatter formatOp operation failed.", e);
            throw e;
        }
    }

    @Override
    public void endTx(DsTransaction dt, DsMetaData dmd, NgFormattedData nfd) throws Exception {
        //NOOP
    }

    @Override
    public void ddlOperation(DsOperation.OpType opType, ObjectType objectType, String objectName, String ddlText) throws Exception {
        // /This is where we should capture schema change for Schema Reg
        schemaGenerator.dropSchema(objectName);
    }
    
    private void formatBeforeValuesOp(DsOperation.OpType type, DsTransaction tx, DsOperation op, 
            TableMetaData tmeta, Struct rec, Struct key){
        formatOperationMetadata(type, op, tmeta, rec);
        formatBeforeValues(tx, op, tmeta, rec);
        formatBeforeKeys(tx, op, tmeta, key);

    }
    
    private void formatAfterValuesOp(DsOperation.OpType type, DsTransaction tx, DsOperation op, 
            TableMetaData tmeta, Struct rec, Struct key){
        formatOperationMetadata(type, op, tmeta, rec);
        formatAfterValues(tx, op, tmeta, rec);
        formatAfterKeys(tx, op, tmeta, key);
        
    }

    
    private void formatBeforeValues(DsTransaction tx, DsOperation op, 
            TableMetaData tMeta, Struct rec){
        int cIndex = 0;
        for(final DsColumn col : op.getColumns()) {
            final ColumnMetaData cMeta = tMeta.getColumnMetaData(cIndex++);
            final DsColumn beforeCol = col.getBefore();
            //Only need to include a value if the before column object is not 
            //null and the associated value is not null, this unmasks a 
            //shortcoming of Avro formatter.  There is no difference between
            //a missing column and a null value.
            if ((beforeCol != null)&&(!beforeCol.isValueNull())){
                //The beforeCol object is NOT null
                formatColumnValue(cMeta, beforeCol, rec);
            }
        }
    }
    
    private void formatBeforeKeys(DsTransaction tx, DsOperation op,
            TableMetaData tmeta, Struct key){
        if (key == null){
            //In this case nothing to do.  Simply return.
            return;
        }
        int cIndex = 0;
        for (final DsColumn col : op.getColumns()){
            final ColumnMetaData cmeta = tmeta.getColumnMetaData(cIndex++);
            if (cmeta.isKeyCol()){
                //This is a primary key column
                final DsColumn beforeCol = col.getBefore();
                if ((beforeCol != null)&&(!beforeCol.isValueNull())){
                    formatColumnValue(cmeta, beforeCol, key);
                }
            }
        }
    }
    
    private void formatAfterValues(DsTransaction tx, DsOperation op, 
            TableMetaData tMeta, Struct rec){
        int cIndex = 0;
        for(final DsColumn col : op.getColumns()) {
            final ColumnMetaData cMeta = tMeta.getColumnMetaData(cIndex++);
            final DsColumn afterCol = col.getAfter();
            //Only need to include a value if the after column object is not 
            //null and the associated value is not null, this unmasks a 
            //shortcoming of Avro formatter.  There is no difference between
            //a missing column and a null value.
            if ((afterCol != null)&&(!afterCol.isValueNull())){
                //The afterCol object is NOT null
                formatColumnValue(cMeta, afterCol, rec);
            }
        }
    }
    
    private void formatAfterKeys(DsTransaction tx, DsOperation op,
            TableMetaData tmeta, Struct key){
        if (key == null){
            //In this case nothing to do.  Simply return.
            return;
        }
        int cIndex = 0;
        for (final DsColumn col : op.getColumns()){
            final ColumnMetaData cmeta = tmeta.getColumnMetaData(cIndex++);
            if (cmeta.isKeyCol()){
                //This is a primary key column
                final DsColumn afterCol = col.getAfter();
                if ((afterCol != null)&&(!afterCol.isValueNull())){
                    formatColumnValue(cmeta, afterCol, key);
                }
            }
        }
    }
    
    protected void formatOperationMetadata(DsOperation.OpType type, DsOperation op, 
            TableMetaData tMeta, Struct rec){
        formatTableName(tMeta, rec);
        formatOpType(type, rec);
        formatOperationTimestamp(op, rec);
        formatCurrentTimestamp(rec);
        formatPosition(op, rec);
        //formatPrimaryKeys(tMeta, rec);
        //formatTokens(op, rec);
    }

    private void formatTableName(TableMetaData tMeta, Struct rec){
        rec.put("table", tMeta.getTableName().getOriginalName());
    }
    
    private void formatOpType(DsOperation.OpType type, Struct rec){ 
        if(type.isInsert()){
            rec.put("op_type", insertOpKey);
        }else if (type.isUpdate()){
            rec.put("op_type", updateOpKey);
        }else if (type.isDelete()){
            rec.put("op_type", deleteOpKey);
        }
    }
    
    private void formatOperationTimestamp(DsOperation op, Struct rec){
        rec.put("op_ts", op.getTimestampAsString());
    }
    
    private void formatCurrentTimestamp(Struct rec){
        rec.put("current_ts", NgUniqueTimestamp.generateUniqueTimestamp(useIso8601Format));
    }
    
    private void formatPosition(DsOperation op, Struct rec){
        rec.put("pos", op.getPosition());
    }

    
    protected void formatColumnValue(ColumnMetaData cMeta, DsColumn col, Struct rec){
		final String fieldName = cMeta.getOriginalColumnName();

        if (treatAllColumnsAsStrings){
            //User has selected to treat all columns as strings
            rec.put(fieldName, col.getValue());
        }else{
            final Object colValue;
            switch (cMeta.getDataType().getGGDataType()){

                //First handle numbers
                case GG_16BIT_S:
                case GG_16BIT_U:
                case GG_32BIT_S:
                case GG_32BIT_U:
                case GG_64BIT_S:
                    if (cMeta.getDataType().getScale() > 0){
                        colValue = Double.valueOf(col.getValue());
                    }else{
                        colValue = Long.valueOf(col.getValue());
                    }
                    break;
                case GG_64BIT_U:
                    colValue = Double.valueOf(col.getValue());
                    break;
                case GG_REAL:
                case GG_IEEE_REAL:
                    colValue = Float.valueOf(col.getValue());
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
                    colValue = Double.valueOf(col.getValue());
                    break;
                case GG_ASCII_V:
                case GG_ASCII_F:
                    // Even though it's coming to us as character data, we need to
                    // inspect the sub-type to see whether it is a number type.  If
                    // this is a number type, we'll use Double.
                    if (cMeta.getDataType().getGGDataSubType() == GG_SUBTYPE_FLOAT ||
                        cMeta.getDataType().getGGDataSubType() == GG_SUBTYPE_FIXED_PREC) {
                        // This is a number data, let's use Double for consistency.
                        colValue = Double.valueOf(col.getValue());
                    } else {
                        colValue = col.getValue();  
                    }
                    break;

                default:
                    //Treat it as a string
		    colValue = col.getValue();
            }
            rec.put(fieldName, colValue);
        }
    }
    
    public void metaDataChanged(DsEvent e, DsMetaData meta) { }
}
