# Strabo 2

In order to run the Data Loader and Query executor, first modify the scope of spark-hive_2.11 dependency in distributed-strabon/pom.xml to "provided" in case the spark-hive libraries are available to the execution environment.

Then run: mvn clean install -DskipTests=true

You should use the jar that has been created in distributed-strabon/target/it.unibz.inf.obda.distributed-strabon-1.16.1-jar-with-dependencies.jar in the spark submit.

For Data Loader:

$SPARK_HOME/bin/spark-submit --class it.unibz.krdb.obda.geofb.DataLoader --executor-memory 116GB --total-executor-cores 31 --master spark://ip:7078 /home/user/it.unibz.inf.obda.distributed-strabon-1.16.1-jar-with-dependencies.jar -i hdfs://ip:9001/user/synthetic/all768/ -o synthetic768 -lp TT,VP -dp false -dropDuplicates false -drdb false -tblfrm Parquet -geom false -df dict -outTripleTable triples

where, -i is the input HDFS directory that contains the NTriples files, -o is the HIVE database name which must have been created prior to execution. The rest of the parameters should be left unchanged.

For Query Executor:

$SPARK_HOME/bin/spark-submit --class it.unibz.krdb.obda.geofb.QueryExecutor --executor-memory 116GB --total-executor-cores 32 --master spark://ip:7078 /home/user/it.unibz.inf.obda.distributed-strabon-1.16.1-jar-with-dependencies.jar hdfs://ip:9001/user/synthetic/queries768/ synthetic768 hdfs://ip:9001/user/synthetic/asWKTTables.txt true true 384

The arguments for Query Executor are as follows:
- Directory in HDFS that contains the queries (e.g. hdfs://ip:9001/user/synthetic/queries768/)
- Hive DB name (e.g. synthetic768)
- Text file in HDFS that contains the nessecary pairs of properties for the geometry linking tables (e.g. hdfs://ip:9001/user/synthetic/asWKTTables.txt)
- Option to push thematic processing before spatial join (e.g. true)
- Option to use spatial index (e.g. true)
- number of Spark repartition setting. In experiments it was set to 5 x number of virtual cores

For Synthetic benchmark, the contents of the asWKTTables.txt are:
> http://geographica.di.uoa.gr/generator/landOwnership/hasGeometry,http://geographica.di.uoa.gr/generator/landOwnership/asWKT
> http://geographica.di.uoa.gr/generator/road/hasGeometry,http://geographica.di.uoa.gr/generator/road/asWKT
> http://geographica.di.uoa.gr/generator/pointOfInterest/hasGeometry,http://geographica.di.uoa.gr/generator/pointOfInterest/asWKT
> http://geographica.di.uoa.gr/generator/state/hasGeometry,http://geographica.di.uoa.gr/generator/state/asWKT

For Scalability benchmark, the contents of the asWKTTables.txt are:
> http://www.opengis.net/ont/geosparql#hasGeometry,http://www.opengis.net/ont/geosparql#asWKT
