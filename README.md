This is a starter kit for creating an application that uses [Spring Batch](http://projects.spring.io/spring-batch/) and
[marklogic-spring-batch](https://github.com/sastafford/marklogic-spring-batch) for ingesting records from a delimited 
file where each record spans multiple lines. Each record then becomes a single document in MarkLogic. 
The intent is to simplify the process of creating an application using Spring Batch by 
leveraging the reusable components in marklogic-spring-batch, and by organizing a Gradle-based project for you that you
can clone/fork/etc to quickly extend and customize for your specific needs. 

This project has the following defaults in place that you can use as a starting point:

1. Defaults to writing to MarkLogic using localhost/8000/admin/admin
1. Defaults to reading the example file data/1000-persons.csv
1. Defaults to combining every 2 lines into 1 document
1. Has a Gradle task for launching the ingest - "./gradlew ingest"

## How do I try this out?

To try this out locally with the default configuration for H2, just do the following:

1. Clone this repo
1. Verify you have ML 8+ or ML 9+ installed locally and that port 8000 (the default one) points to the Documents database 
(you can of course modify this to write to any database you want)
1. Verify that the username/password properties in gradle.properties are correct for your MarkLogic cluster (it's best 
not to use the admin user unless absolutely necessary, but this defaults to it for the sake of convenience)
1. Run ./gradlew ingest

You should see some logging like this:

    14:20:54.770 [main] INFO  org.example.IngestConfig - API: rest
    14:20:54.773 [main] INFO  org.example.IngestConfig - Chunk size: 100
    14:20:54.773 [main] INFO  org.example.IngestConfig - Hosts: localhost
    14:20:54.773 [main] INFO  org.example.IngestConfig - Root local name: persons
    14:20:54.773 [main] INFO  org.example.IngestConfig - Child record name: person
    14:20:54.773 [main] INFO  org.example.IngestConfig - Collections: example
    14:20:54.773 [main] INFO  org.example.IngestConfig - Permissions: rest-reader,read,rest-writer,update
    14:20:54.773 [main] INFO  org.example.IngestConfig - Thread count: 16
    14:20:54.773 [main] INFO  org.example.IngestConfig - Input file path: data/1000-persons.csv
    14:20:54.773 [main] INFO  org.example.IngestConfig - Row count: 2
    14:20:54.788 [main] INFO  org.example.IngestConfig - Client username: admin
    14:20:54.788 [main] INFO  org.example.IngestConfig - Client database: Documents
    14:20:54.788 [main] INFO  org.example.IngestConfig - Client authentication: DIGEST
    14:20:54.788 [main] INFO  org.example.IngestConfig - Creating client for host: localhost
    14:20:55.001 [main] INFO  org.example.IngestConfig - Initialized components, launching job
    14:20:55.073 [main] INFO  c.m.s.b.i.writer.MarkLogicItemWriter - On stream open, initializing BatchWriter
    14:20:55.564 [main] INFO  c.m.s.b.i.writer.MarkLogicItemWriter - On stream open, finished initializing BatchWriter
    14:20:55.816 [main] INFO  c.m.s.b.i.writer.MarkLogicItemWriter - On stream close, waiting for BatchWriter to complete
    14:20:56.273 [main] INFO  c.m.s.b.i.writer.MarkLogicItemWriter - On stream close, finished waiting for BatchWriter to complete
    14:20:56.273 [main] INFO  c.m.s.b.i.writer.MarkLogicItemWriter - Final Write Count: 500

When using the sample H2 database, you only need to run "setupH2" once. And of course, when you're using your own 
database, you can remove this task from build.gradle.

The configuration properties are all in gradle.properties. You can modify those properties on the command line
via Gradle's -P mechanism. For example, to load the data as JSON instead of XML:

    ./gradlew ingest -Pdocument_type=json

If you have ML9, you can try out the new Data Movement SDK (DMSDK):

    ./gradlew ingest -Papi=dmsdk

Or load the data via XCC instead of the REST API:

    ./gradlew ingest -Papi=xcc

For both the REST API and XCC, you can specify multiple hosts to send requests to:

    ./gradlew ingest -Phosts=host1,host2,host3

You can easily modify the thread count and chunk size:

    ./gradlw ingest -Pchunk=50 -Pthread_count=32

And you can modify the row count:

    ./gradlew ingest -Prow_count=17

Or provide your own file:

    ./gradlew ingest -Pinput_file_path=/path/to/my.csv

Or customize the root and child element names:

    ./gradlew ingest -Proot_local_name=my-root -Pchild_record_name=my-child

Or just modify gradle.properties and start building your own application. 

You can also see all the supported arguments:

    ./gradlew help

### But how do I modify the XML that's inserted into MarkLogic?

The way the batch job works is defined by the org.example.IngestConfig class. This class creates a Spring Batch
Reader, Writer, and Processor (for more information on these concepts, definitely check out the 
[Spring Batch user manual](http://docs.spring.io/spring-batch/reference/html/)). 

The XML is currently generated by the org.example.ColumnMapProcessor class. This is a quick-and-dirty Spring Batch
Processor implementation that uses a simple [StAX](https://docs.oracle.com/javase/tutorial/jaxp/stax/api.html)-based
approach for converting a Spring ColumnMap (a map of column names and values; I'm using this term as shorthand for a 
Map<String, Object>) into an XML document. 

To modify how this works, you'll need to write code, which opens the door to all the batch-processing power and 
flexibility provided by Spring Batch. Here are a few paths to consider:

1. Modify the ColumnMapProcessor with your own method for converting a ColumnMap into a String of XML, or JSON
1. Write your own Processor implementation from scratch and use that, and modify IngestConfig to use your Processor
1. Write your own Reader that returns something besides a ColumnMap. Modify IngestConfig to use this new Reader, 
and you'll need to modify the Processor as well, which expects a ColumnMap.
1. You can even replace the Writer, which depends on a MarkLogic Java Client DocumentWriteOperation instance. Typically
though, you'll be able to retain this part by having your Reader and/or Processor return a DocumenteWriteOperation, 
which encapsulates all the information needed to write a single document to MarkLogic.