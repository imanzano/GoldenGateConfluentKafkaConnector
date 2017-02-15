/*
 *
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.goldengate.kafkaconnect.formatter;

/**
 * Enumerate the behavior options when a PK (primary key) update is encountered.
 * PK_ABEND - ABEND
 * PK_UPDATE - Treat it as a normal update.
 * PK_DELETE_INSERT - Treat it as a delete and an insert.
 * @author tbcampbe
 */
public enum PkHandling{
    PK_ABEND ("abend"),
    PK_UPDATE ("update"),
    PK_DELETE_INSERT ("delete-insert");

    private final String action;

    /**
     * Create the PKHandling object.
     * @param a 
     */
    PkHandling(String a){
        action = a;
    }

    /**
     * Method to compare actions.
     * @param a The action.
     * @return true if same, else false.
     */
    public boolean compareAction(String a){
        return action.equalsIgnoreCase(a);
    }
}
