package com.kylebarron.stataParquet;

import com.stata.sfi.SFIToolkit;
import com.stata.sfi.Data;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static com.kylebarron.stataParquet.io.InputFile.nioPathToInputFile;

/*
    Example of reading writing Parquet in java without BigData tools.
    Run from Stata with:

    javacall com.kylebarron.stataParquet.DataLoad printParquet, jar(stataParquet.jar)
*/
public class DataLoad {

  public static int sayhello(String[] args)
    {
        SFIToolkit.displayln("Hello from java!");
        return(0);      // Stata return code
    }

  public static int printParquet(String[] args) {
    try {
      SFIToolkit.displayln("Inside printParquet!");
      final Path parquetFilePath = FileSystems.getDefault().getPath("sample.parquet");
      readFromParquet(parquetFilePath);
      return(0);
    } catch (Throwable e) {
      return(1);
    }
  }

  private static void readFromParquet(@Nonnull final Path filePathToRead) throws IOException {
    try (final ParquetReader<GenericData.Record> reader = AvroParquetReader
            .<GenericData.Record>builder(nioPathToInputFile(filePathToRead))
            .withConf(new Configuration())
            .build())
    {
      SFIToolkit.displayln("Inside readFromParquet!");
      GenericData.Record record;
      long i = 0;
      while ((record = reader.read()) != null) {
        i += 1;
        if (i == 1) {
            // Set to 4 observations
            Data.setObsTotal(4);

            // Create columns in Stata
            // Dataset with two integer columns
            Data.addVarInt("index");
            Data.addVarInt("col");
        }

        SFIToolkit.displayln(record.toString());
        SFIToolkit.displayln(record.getSchema().toString());

        // First column in parquet file named a
        SFIToolkit.displayln("coercing a to an int");
        int value_a = (Integer) record.get('a');
        SFIToolkit.displayln("coercing a to a double");
        double double_value_a = (double) value_a;
        SFIToolkit.displayln("storing a in Stata");
        int col = 1;
        Data.storeNum(col, i, double_value_a);

        // First column in parquet file named b
        SFIToolkit.displayln("coercing b to an int");
        int value_b = (Integer) record.get('b');
        SFIToolkit.displayln("coercing b to a double");
        double double_value_b = (double) value_b;
        SFIToolkit.displayln("storing b in Stata");
        int col2 = 2;
        Data.storeNum(col2, i, double_value_b);
      }
    }
  }
}
