package com.kylebarron.stataParquet;

import com.stata.sfi.SFIToolkit;
import com.stata.sfi.Data;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static com.kylebarron.stataParquet.io.InputFile.nioPathToInputFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * Read Parquet file into Stata
 *
 * @author Kyle Barron
 * @version 0.0.0
 * javacall com.kylebarron.stataParquet.ParquetStataReader read, jar(stataParquetShaded.jar)
 */
public class ParquetStataReader {

  private static ParquetMetadata getMetadata(String pathString) {
    try {
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(pathString);
      boolean loadDefaults = false;
      Configuration configuration = new Configuration(loadDefaults);
      ParquetMetadata metadata = ParquetFileReader.readFooter(configuration,
      path, ParquetMetadataConverter.NO_FILTER);
      return metadata;
    } catch (Throwable e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String sStackTrace = sw.toString();
      SFIToolkit.displayln(sStackTrace);
      return null;
    }
  }

  private static long getRows(ParquetMetadata metadata) {
    long num_rows = 0;
    List<BlockMetaData> blocks = metadata.getBlocks();
    for(BlockMetaData b: blocks) {
      num_rows += b.getRowCount();
    }
    return num_rows;
  }

  private static Map<String, Type> getColumns(ParquetMetadata metadata) {

    Map<String, Type> dictionary = new HashMap<String, Type>();
    MessageType schema = metadata.getFileMetaData().getSchema();

    List<Type> fields = schema.getFields();
    for (Type field: fields) {
      String name = field.getName();
      Type type = schema.getType(name);
      dictionary.put(name, type);
    }
    return dictionary;
  }

  private static void createColumnsStata(Map<String, Type> columns) {
    for (Map.Entry<String, Type> entry : columns.entrySet())
    {
      String name = entry.getKey();
      Type type = entry.getValue();

      Data.addVarLong(name);
    }
  }

  private static void setStataObs(ParquetMetadata metadata) {
    long num_rows = getRows(metadata);
    Data.setObsTotal(num_rows);
  }

  public static int read(String[] args) {
    try {
      String filePath = "sample2.parquet";
      ParquetMetadata metadata = getMetadata(filePath);

      // Set number of observations in data
      setStataObs(metadata);

      // Add columns
      Map<String, Type> columns = getColumns(metadata);
      createColumnsStata(columns);

      // Store data in Stata
      final Path parquetFilePath = FileSystems.getDefault().getPath(filePath);
      readFromParquet(parquetFilePath, columns);

      return(0);
    } catch (Throwable e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String sStackTrace = sw.toString();
      SFIToolkit.displayln(sStackTrace);
      return(1);
    }
  }

  public static int sayhello(String[] args) {
    SFIToolkit.displayln("Hello from java!") ;
    return(0);
  }

  private static void readFromParquet(@Nonnull final Path filePathToRead, Map<String, Type> columns) throws IOException {
    try (final ParquetReader<GenericData.Record> reader = AvroParquetReader
            .<GenericData.Record>builder(nioPathToInputFile(filePathToRead))
            .withConf(new Configuration())
            .build())
    {

      SFIToolkit.displayln("Inside readFromParquet!");
      GenericData.Record record;

      long rowNum = 0;
      while ((record = reader.read()) != null) {
        rowNum += 1;

        int colNum = 0;
        for (Map.Entry<String, Type> entry : columns.entrySet()) {
          colNum += 1;

          String name = entry.getKey();
          Type type = entry.getValue();

          long value = (Long) record.get(name);
          double d = (double) value;
          Data.storeNum(colNum, rowNum, d);
        }
      }
    }
  }
}
