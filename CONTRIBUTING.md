


In order to install the Stata JAR onto my system, I ran the following, where
`-Dfile=sfi-api.jar` should be replaced with the path to the `sfi-api.jar` of
your Stata installation.


```
> mvn install:install-file -DgroupId=com.stata -DartifactId=sfi -Dversion=15 -Dpackaging=jar -Dfile=sfi-api.jar

[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building stataParquet 1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-install-plugin:2.4:install-file (default-cli) @ stataParquet ---
[INFO] Installing /home/kyle/github/stata/stataParquet/sfi-api.jar to /home/kyle/.m2/repository/com/stata/sfi/15/sfi-15.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 0.272 s
[INFO] Finished at: 2018-11-01T17:39:39-04:00
[INFO] Final Memory: 8M/481M
[INFO] ------------------------------------------------------------------------
```


Need to use "Shaded" jar for things to work correctly. This is the jar in which hadoop confs have been merged.


### Source documentation

Apache Avro API docs are here: https://avro.apache.org/docs/1.8.2/api/java/org/apache/avro/package-summary.html

`parquet-avro` API docs are here: https://www.javadoc.io/doc/org.apache.parquet/parquet-avro/1.10.0

`parquet-column` API docs are here: https://www.javadoc.io/doc/org.apache.parquet/parquet-column/1.10.0

Stata API docs are here: https://www.stata.com/java/

To run javadocs, run:
```
javadoc -d docs org.apache.parquet -sourcepath src/main/java/
```
where the sourcepath is the folder that contains `org/apache/parquet/`.
