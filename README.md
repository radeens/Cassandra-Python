#Python Application for Cassandra Databases.

##Resources:          
                      Ubuntu 14.x.x 
                      Cassandra 2.0
                      Cassandra Driver
                      Python 2.7.0
          
##Bringing up Cassandra

Cassandra is an open-source distributed database management system which is designed to scale to handle large volumes of data processed across a large number of commodity machines. Cassandra's data model is essentially a Key-Value Store wherein data is stored as a partitioned row store with tunable consistency. Each key in Cassandra corresponds to a value which is an object. Each key has values as columns, and columns are grouped together into sets called column families.

##Data Model

Rows are organized into tables where the first component of a table's primary key is the partition key. Within a partition, rows are clustered by the remaining columns of the key. Other columns may be indexed separately from the primary key. More information about the features, data model, and support for clustering can be found here: Info

##Cassandra Query Language

Cassandra is accessed using the Cassandra Query Language CQL (current version 3), which is similar to SQL, with a few major omissions. The CQL3 documentation for the current version of Cassandra (2.0) is available here.
          
