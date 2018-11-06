# stataParquet

A prototype Java implementation of reading Parquet files into Stata.

There is currently very basic read support. To build, install Java 8, Maven, and Git and run:

```
git clone https://github.com/kylebarron/stataParquet
mvn package
```

Then copy or symlink the file

```
target/stataParquet-0.1.0-jar-with-dependencies.jar
```
to whatever your `PERSONAL` directory is in Stata.

Then run:

```
javacall com.kylebarron.stataParquet.ParquetStataReader read, ///
    jar(stataParquetShaded.jar) ///
    args("path/to/parquet/file.parquet")
```
