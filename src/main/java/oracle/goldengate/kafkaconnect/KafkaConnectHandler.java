/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect;


import oracle.goldengate.datasource.DsConfiguration;
import oracle.goldengate.datasource.DsEvent;
import oracle.goldengate.datasource.DsOperation;
import oracle.goldengate.datasource.DsTransaction;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.ObjectType;
import oracle.goldengate.datasource.TxOpMode;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.adapt.Tx;
import oracle.goldengate.datasource.handler.NgFormattedOutputHandler;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;

import oracle.goldengate.format.NgFormattedData;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import oracle.goldengate.kafkaconnect.formatter.KafkaConnectFormattedData;

import oracle.goldengate.util.GGException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the next generation Kafka Handler for the 12.2 OGGADP Big Data
 * release. This implementation uses the core of the Kafka Connect 
 * framework.
 *
 * @author tbcampbe
 */
public class KafkaConnectHandler extends NgFormattedOutputHandler {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConnectHandler.class);    
    private final GGProperties kafkaProperties;
    private GGProducer kafkaProd=null;
    private SourceRecordGenerator createPR=null;
    private final ConfluentHandlerMetrics handlerMetrics;

    /**
     * Default no-arg Constructor
     */
    public KafkaConnectHandler() {
        this(TxOpMode.tx);
    }
    
    /**
     * Constructor with mode.
     * @param m tx for transaction mode, op for operation mode.
     */
    public KafkaConnectHandler(TxOpMode m) {
        super(m);
        kafkaProperties = new GGProperties();
        handlerMetrics = new ConfluentHandlerMetrics();
    }
  
    /**
     * Initialize the Confluent Kafka handler and its producer with the kafka-producer
     * properties.
     *
     * @param conf The configuration object
     * @param metaData The metadata object
     */
    @Override
    public void init(DsConfiguration conf, DsMetaData metaData) {
        super.init(conf, metaData);
        kafkaProperties.loadKafkaProperties();

        if(logger.isInfoEnabled()) {
            final StringBuilder sb = new StringBuilder();
            sb.append(System.lineSeparator());
            sb.append("**** Kafka Handler (Connect Framework) - Configuration Summary ****");
            sb.append(System.lineSeparator());
            //The Kafka producer configuration file
            sb.append("  Kafka Producer Config file is set to: ");
            sb.append(kafkaProperties.getKafkaProducerConfigFile());
            sb.append(System.lineSeparator());
            //The Kafka Producer Record Creator Class
            sb.append("  Kafka Producer Record Creator class is set to: ");
            sb.append(kafkaProperties.getSourceRecordGeneratorClassName());
            sb.append(System.lineSeparator());
            //Mode of operation
            sb.append("  Kafka Handler is running in mode: ");
            sb.append(getMode().name());
            sb.append(System.lineSeparator());
            //Output the content of the Kafka producer config file
            sb.append("  Contents of Kafka producer configuration file ");
            sb.append(System.lineSeparator());
            kafkaProperties.printKafkaConnectionProps(sb);
      
            sb.append("**** End Kafka Handler (Connect Framework) - Configuration Summary ****");
            sb.append(System.lineSeparator());
            logger.info(sb.toString());
        }
        //Generate the Confluent Kafka producer
        kafkaProd = kafkaProperties.instantiateConfluentKafkaProducer();
        //Instantiate the SourceRecordGenerator
        createPR = kafkaProperties.getSourceRecordGenerator();
    }

    /**
     * Handler the new operation received event along with any possible
     * formatting that may be required to be done.
     *
     * @param e The event
     * @param tx The transaction object
     * @param op The current operation object
     * @return Status.OK for sucess, else other
     */
    @Override
    public Status operationAdded(DsEvent e, DsTransaction tx, DsOperation op) {
        super.operationAdded(e, tx, op);

        Status status = Status.OK;
        // In operation mode operation data is processed immediately as the operation
        // is received.  In transaction mode the operations are cached and then
        // processed in the transaction commit call.
        if (isOperationMode()) {
            final KafkaConnectFormattedData data = (KafkaConnectFormattedData) formatter.createNgFormattedData();
            // Tx/Op/Col adapters wrap metadata & values behind a single, simple
            // interface if using the DataSourceListener API (via AbstractHandler).
            final Tx txAdapt = new Tx(tx, getMetaData(), getConfig());
            final TableMetaData tMeta = getMetaData().getTableMetaData(op.getTableName());
            final Op opAdapt = new Op(op, tMeta, getConfig());
            //Increment the op counters
            incrementCounters(opAdapt);
            //Format the data
            status = formatOp(txAdapt, opAdapt, data); 

            if (status == Status.OK) {
                for (int i = 0; i < data.size(); i++) {
                    final Struct record = data.getRecord(i);
                    final Struct key = data.getKey(i);
                    status = processData(txAdapt, opAdapt, key, record);
                    if (status != Status.OK){
                        break;
                    }
                }
            }
        }

        return status;
    }

    /**
     * Handle the transaction commit event along with any possible formatting
     * that may be required to be done at this time.
     *
     * @param e The event object
     * @param tx The transaction object.
     * @return Status.OK for success, else failure
     */
    @Override
    public Status transactionCommit(DsEvent e, DsTransaction tx) {
        Status status = super.transactionCommit(e, tx);
        final Tx txAdapt = new Tx(tx, getMetaData(), getConfig()); 
        if(!isOperationMode()) {
            for(final DsOperation op : tx.getOperations()) {
                final TableMetaData tMeta = getMetaData().getTableMetaData(op.getTableName());
                final Op opAdapt = new Op(op, tMeta, getConfig());
                final KafkaConnectFormattedData data = (KafkaConnectFormattedData) formatter.createNgFormattedData();
                //Increment the op counters
                incrementCounters(opAdapt);
                //Format the data
                status = formatOp(txAdapt, opAdapt, data); 

                if (status == Status.OK) {

                    for (int i = 0; i < data.size(); i++) {
                        final Struct record = data.getRecord(i);
                        final Struct key = data.getKey(i);
                        status = processData(txAdapt, opAdapt, key, record);
                        if (status != Status.OK){
                            break;
                        }
                    }
                }
                if (status != Status.OK){
                    break;
                }
            }
        }
        if (status == Status.OK){
            //Increment the number of transactions
            handlerMetrics.incrementNumTxs();
            //Flush Kafka on the transaction commit boundary to ensure write durability
            status = kafkaProd.flush();
        }
        return status;
    }
    
    /**
     * Handle the ddlOperation event along with any possible formatting that may
     * be required to send across the ddl statement as a possible message.
     *
     * @param opType The operation type
     * @param objectType The object type
     * @param objectName The fully qualified table name
     * @param ddlText The DDL text
     * @return Status.OK for success, other for failure
     */
    @Override
    public Status ddlOperation(DsOperation.OpType opType, ObjectType objectType, String objectName, String ddlText) {
        handlerMetrics.incrementNumDdlOps();
        logger.info("DDL operation received for table [" + objectName + "] DDL [" + ddlText + "].");
        return super.ddlOperation(opType, objectType, objectName, ddlText);
    }

    /**
     * Handle the metadataChanged events. TODO: Schema topics: Can possibly send
     * avro metadata schemas to a persistent kafka topic.
     *
     * @param e The event object.
     * @param meta The metadata object
     * @return Status.OK for success
     */
    @Override
    public Status metaDataChanged(DsEvent e, DsMetaData meta) {
        final Status status = super.metaDataChanged(e, meta);
        //Need to clear out cached metadata
        final TableMetaData tMeta = (TableMetaData)e.getEventSource();
        final String tableName = tMeta.getTableName().getOriginalName();
        //Need to get this to the formatter.
        return status;
    }

    /**
     * Method to set the Kafka producer properties file.  The file must be on
     * the classpath to be loaded.  Configure using the following parameter
     * in the GoldenGate Java Properties file.
     * gg.handler.name.kafkaProducerConfigFile
     * @param fileName The Kafka producer properties file.
     */
    public void setKafkaProducerConfigFile(String fileName) {
        kafkaProperties.setKafkaProducerConfigFile(fileName);
    }

    /**
     * Method to set the SourceRecordGenerator implementation class.
     * Setting this parameter is optional and if not set the
     * DefaultSourceRecordGenerator class will be used.
     * Configure using the following parameter in the GoldenGate Java properties
     * file:
     * gg.handler.name.sourceRecordGeneratorClass
     * @param className The fully qualified class name of the SourceRecordGenerator
     * implementation class.
     */
    public void setSourceRecordGeneratorClass(String className) {
        kafkaProperties.setSourceRecordGeneratorClass(className);
    }   

    /**
     * This method is responsible to process and format each operation as it
     * comes. Also creates a new producerRecord for each operation and submits
     * it to the Kafka producer.
     *
     * @param tx The current transaction
     * @param op The current operation
     * @return Status.OK if success
     */
    private Status formatOp(Tx tx, Op op, NgFormattedData data) {
        if (logger.isDebugEnabled()) {
            logger.debug("Process operation: table=[" + op.getTableName() + "]"
                    + ", op pos=" + op.getPosition()
                    + ", tx pos=" + tx.getTranID()
                    + ", op ts=" + op.getTimestamp());
        }

        final TableMetaData tMeta = getMetaData().getTableMetaData(op.getTableName());

        try {
            formatter.formatOp(tx.getTransaction(), op.getOperation(), tMeta, data);
           
        } catch (Exception e) {
            logger.error("Confluent Kafka Handler failed to format and process operation: table=[" + op.getTableName() + "]"
                    + ", op pos=" + op.getPosition()
                    + ", tx pos=" + tx.getTranID()
                    + ", op ts=" + op.getTimestamp(), e);
            throw new GGException("Confluent Kafka Handler failed to format and process operation: table=[" + op.getTableName() + "]"
                    + ", op pos=" + op.getPosition()
                    + ", tx pos=" + tx.getTranID()
                    + ", op ts=" + op.getTimestamp(), e);
        }
        return Status.OK;
    }

    /**
     * This method is responsible for creating the Kafka producer record
     * and submitting it to the Kafka producer.
     * 
     * @param tx The current transaction
     * @param op The current operation
     * @param key The Kafka Connect key struct
     * @param payload The Kafka Connect Payload struct
     * @return Status.OK for success, else any other status.
     */
    private Status processData(Tx tx, Op op, Struct key, Struct payload) {
        final SourceRecord sr =
                createPR.createSourceRecord(tx, op, kafkaProd.getKafkaProducer(), key, payload);
        return kafkaProd.send(sr);
    }
   
    /**
     * Destroys the Kafka Producer and calls super methods for cleanup.
     */
    @Override
    public void destroy() {
        logger.debug("Kafka Connect Handler destroy");
        super.destroy();
        //Flush just in case
        kafkaProd.flush();
        kafkaProd.close();
    }
    
    @Override
    public String reportStatus() {
        logger.debug("Invoked Confluent Kafka Handler method \"reportStatus\".");
        final StringBuilder sb = new StringBuilder();
        sb.append("Status report: mode=").append(getMode());
        sb.append(", transactions=").append(handlerMetrics.getNumTxs());
        sb.append(", operations=").append(handlerMetrics.getNumOps());
        sb.append(", inserts=").append(handlerMetrics.getNumInserts());
        sb.append(", updates=").append(handlerMetrics.getNumUpdates());
        sb.append(", PK updates=").append(handlerMetrics.getNumPkUpdates());
        sb.append(", deletes=").append(handlerMetrics.getNumDeletes());
        sb.append(", truncates=").append(handlerMetrics.getNumTruncates());
        sb.append(", ddl operations=").append(handlerMetrics.getNumDdlOps());
        return sb.toString();
    }
    

    private void incrementCounters(Op op){
        if(op.getOpType().isInsert()){
            handlerMetrics.incrementNumInserts();
        }else if(op.getOpType().isPkUpdate()){
            handlerMetrics.incrementNumPkUpdates();
        }else if(op.getOpType().isUpdate()){
            handlerMetrics.incrementNumUpdates();
        }else if(op.getOpType().isDelete()){
            handlerMetrics.incrementNumDeletes();
        }else if(op.getOpType().isTruncate()){
            handlerMetrics.incrementNumTruncates();
        }
    }
}
