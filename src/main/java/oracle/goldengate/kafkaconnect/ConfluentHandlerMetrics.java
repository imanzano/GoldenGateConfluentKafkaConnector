/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect;

/**
 * Counters to track transactions, operations, inserts, updates, pk updates,
 * deletes, truncates, and DDL operations.
 * @author tbcampbe
 */
public class ConfluentHandlerMetrics {
    private long numTxs = 0;
    private long numOperations = 0;
    private long numInserts = 0;
    private long numUpdates = 0;
    private long numPkUpdates = 0;
    private long numDeletes = 0;
    private long numTruncates = 0;
    private long numDdlOperations = 0;
    
    /**
     * Method to increment the number of transactions.
     */
    public void incrementNumTxs(){
        numTxs++;
    }
    
    /**
     * Method to get the number of transactions processed.
     * @return The number of transactions processed.
     */
    public long getNumTxs(){
        return numTxs;
    }
    
    /**
     * Method to get the total number of operations processed.
     * @return Total number of operations processed.
     */
    public long getNumOps(){
        return numOperations;
    }
    
    /**
     * Method to increment the number of inserts.
     */
    public void incrementNumInserts(){
        numOperations++;
        numInserts++;
    }
    
    /**
     * Method to get the number of insert operations processed.
     * @return Total number of insert operations processed.
     */
    public long getNumInserts(){
        return numInserts;
    }
    
    /**
     * Method to increment the number of update operations processed.
     */
    public void incrementNumUpdates(){
        numOperations++;
        numUpdates++;
    }
    
    /**
     * Method to get the number of updates processed.
     * @return Total number of update operations processed.
     */
    public long getNumUpdates(){
        return numUpdates;
    }
    /**
     * Method to increment the number of PK (primary key) updates.
     */
    public void incrementNumPkUpdates(){
        numOperations++;
        numPkUpdates++;
    }
    
    /**
     * Method to get the count of PK (primary key) updates.
     * @return Total number of PK updates processed.
     */
    public long getNumPkUpdates(){
        return numPkUpdates;
    }
    
    /**
     * Method to increment the number of deletes.
     */
    public void incrementNumDeletes(){
        numOperations++;
        numDeletes++;
    }
    
    /**
     * Method to get the number of delete operations.
     * @return Total number of delete operations processed.
     */
    public long getNumDeletes(){
        return numDeletes;
    }
    
    /**
     * Method to increment the number of truncation operations.
     */
    public void incrementNumTruncates(){
        numOperations++;
        numTruncates++;
    }
    
    /**
     * Method to get then number of truncate operations.
     * @return Number of truncate operations processed.
     */
    public long getNumTruncates(){
        return numTruncates;
    }
    
    /**
     * Method to increment the number of DDL events
     */
    public void incrementNumDdlOps(){
        numDdlOperations++;
    }
    
    /**
     * Method to get the number of DDL events
     * @return Total number of DDL events.
     */
    public long getNumDdlOps(){
        return numDdlOperations;
    }
    
}
