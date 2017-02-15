*****************************************************************************************************
This software is distributed under COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) 
Copy of this License can be obtained from https://oss.oracle.com/licenses/CDDL
*****************************************************************************************************

Oracle GoldenGate for Kafka Connect Handler
Version 1.0

-------------
Functionality
-------------

The Kafka Connect Handler takes change data capture operations from a source trail file and generates data structs (org.apache.kafka.connect.data.Struct) as well as the associated schemas (org.apache.kafka.connect.data.Schema). The data structs are serialized via configured converters then enqueued onto Kafka topics. The topic name used corresponds to the fully qualified source table name as obtained from the GoldenGate trail file. Individual operations consist of inserts, updates, and delete operations executed on the source RDBMS. Insert and update operation data include the after change data. Delete operations include the before change data. A primary key update is a special case for an update where one or more of the primary key(s) is/are changed. The primary key update represents a special case in that without the before image data it is not possible to determine what row is actually changing when only in possession of the after change data. The default behavior of a primary key update is to ABEND in the Kafka Connect formatter. However, the formatter can be configured to simply treat these operations as regular updates or to treat them as deletes and then an insert which is the closest big data modeling to the substance of the transaction.

-------------------------------------------------------------
Differences from Kafka Handler in Released GoldenGate Product
-------------------------------------------------------------

The Kafka Handler officially released in Oracle GoldenGate for Big Data 12.2.0.1.0 is slightly different in functionality than the Kafka Connect Handler/Formatter included here. 
The officially released Kafka Handler interfaces with pluggable formatters to output the data to Kafka in XML, JSON, Avro, or delimited text format.
The Kafka Connect Handler/Formatter builds up Kafka Connect Schemas and Structs.  It relies on the Kafka Connect framework to perform the serialization using the Kafka Connect converters before putting the data to topic.

------------------
Supported Versions
------------------

The Oracle GoldenGate Kafka Connect Handler/Formatter is coded and tested with the following product versions.

- Oracle GoldenGate for Big Data 12.2.0.1.1
- Confluent IO Kafka/Kafka Connect 0.9.0.1-cp1

Porting may be required for Oracle GoldenGate Kafka Connect Handler/Formatter to work with other versions of Oracle GoldenGate for Big Data and/or Confluent IO Kafka/Kafka Connect

-------------
Documentation
-------------

Documentation on how to configure and use the Kafka Connect Handler/Formatter is available in the
OGG_Kafka_Connect.pdf file.


------------
Installation
------------

The Oracle GoldenGate Kafka Connect Handler/Formatter should be extracted from its tar file into an empty directory.  The user then can use the sample conf.prm (replicat properties file) and the sample conf.props (Java Adapter Properties file) as a starting point for configuration.
Users need to modify the gg.classpath configuration in the Java Adapter Properties file so that the Kafka Connect Handler/Formatter is loadable at runtime by the JVM.  Also the Kafka
and Kafka Connect dependencies need to be resolved.  See Run Time Dependencies below.


---------------------
Run Time Dependencies
---------------------

Depends on GoldenGate Java Adapter and Big Data jars.  This is typically located at the following:
{GoldenGate Home Directory}/ggjava/ggjava.jar  The ggjava.jar needs to be included in the JVM bootstrap
classpath.

The following need to be in the gg.classpath in the Java Properties file.

The OGG Kafka Connect Adapter/Formatter jar.  This needs to be included in the gg.classpath properties.
ogg-kafka-connect-1.0.jar

The directory containing the Kafka Producer Configuration File.  The name of this file is configured
in the Java Adapter Properties file but the path (without trailing wildcard) must be included in the 
gg.classpath so that the file can be accessed and read at runtime.

Depends on Kafka 0.9.0.1-cp1 Kafka Client jars, Kafka Connect jars, and Kafka Connect JSON jars:

activation-1.1.jar
connect-api-0.9.0.1-cp1.jar
connect-json-0.9.0.1-cp1.jar
generated-sources
jackson-annotations-2.5.0.jar
jackson-core-2.5.4.jar
jackson-databind-2.5.4.jar
jline-0.9.94.jar
jopt-simple-3.2.jar
junit-3.8.1.jar
kafka_2.11-0.9.0.1-cp1.jar
kafka-clients-0.9.0.1-cp1.jar
log4j-1.2.15.jar
lz4-1.2.0.jar
mail-1.4.jar
metrics-core-2.2.0.jar
netty-3.7.0.Final.jar
scala-library-2.11.7.jar
scala-parser-combinators_2.11-1.0.4.jar
scala-xml_2.11-1.0.4.jar
slf4j-api-1.7.6.jar
slf4j-log4j12-1.7.6.jar
snappy-java-1.1.1.7.jar
zkclient-0.7.jar
zookeeper-3.4.6.jar



