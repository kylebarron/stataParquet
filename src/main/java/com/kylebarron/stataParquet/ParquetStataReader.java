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
import org.apache.parquet.schema.OriginalType;
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

  private static Map<String, OriginalType> getColumns(ParquetMetadata metadata) {

    Map<String, OriginalType> dictionary = new HashMap<String, OriginalType>();
    MessageType schema = metadata.getFileMetaData().getSchema();

    List<Type> fields = schema.getFields();
    for (Type field: fields) {
      String name = field.getName();
      Type type = schema.getType(name);
      SFIToolkit.displayln(type.toString());

      OriginalType originalType = schema.getType(name).getOriginalType();
      SFIToolkit.displayln("Got original type");

      // SFIToolkit.displayln(originalType.toString());
      dictionary.put(name, originalType);
    }
    return dictionary;
  }

  private static void createColumnsStata(Map<String, OriginalType> columns) {
    for (Map.Entry<String, OriginalType> entry : columns.entrySet())
    {
      String name = entry.getKey();
      OriginalType originalType = entry.getValue();

      switch (originalType){
        case UTF8:
          Data.addVarStr(name, 1);
          break;
        case DECIMAL:
          Data.addVarDouble(name);
          break;
        case INT_8:
          Data.addVarByte(name);
          break;
        case INT_16:
          Data.addVarInt(name);
          break;
        case INT_32:
          Data.addVarLong(name);
          break;
        case INT_64:
          Data.addVarDouble(name);
          break;
        case UINT_8:
          Data.addVarInt(name);
          break;
        case UINT_16:
          Data.addVarLong(name);
          break;
        case UINT_32:
          Data.addVarDouble(name);
          break;
        case UINT_64:
          Data.addVarDouble(name);
          break;
        default:
          // TODO: Fix the default case
          Data.addVarDouble(name);
      }
    }
  }

  private static void setStataObs(ParquetMetadata metadata) {
    long num_rows = getRows(metadata);
    Data.setObsTotal(num_rows);
  }

  public static int read(String[] args) {
    try {
      String filePath = "sample.parquet";
      ParquetMetadata metadata = getMetadata(filePath);

      // Set number of observations in data
      setStataObs(metadata);
      SFIToolkit.displayln("Finished setting obs!");

      // Add columns
      Map<String, OriginalType> columns = getColumns(metadata);
      SFIToolkit.displayln("Finished retrieving columns!");
      SFIToolkit.displayln(columns.toString());
      createColumnsStata(columns);
      SFIToolkit.displayln("Finished adding columns!");

      // Store data in Stata
      final Path parquetFilePath = FileSystems.getDefault().getPath(filePath);
      readFromParquet(parquetFilePath, columns);
      SFIToolkit.displayln("Finished storing data!");

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

  private static void readFromParquet(@Nonnull final Path filePathToRead, Map<String, OriginalType> columns) throws IOException {
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
        for (Map.Entry<String, OriginalType> entry : columns.entrySet()) {
          colNum += 1;

          String name = entry.getKey();
          OriginalType originalType = entry.getValue();

          switch (originalType){
            case UTF8: {
              String value = record.get(name).toString();
              Data.storeStr(colNum, rowNum, value);
              break;
            }
            case DECIMAL: {
              Double value = (Double) record.get(name);
              Data.storeNum(colNum, rowNum, value);
              break;
            }
            case INT_8: {
              long value = (Long) record.get(name);
              double d = (double) value;
              Data.storeNum(colNum, rowNum, d);
              break;
            }
            case INT_16: {
              long value = (Long) record.get(name);
              double d = (double) value;
              Data.storeNum(colNum, rowNum, d);
              break;
            }
            case INT_32: {
              long value = (Long) record.get(name);
              double d = (double) value;
              Data.storeNum(colNum, rowNum, d);
              break;
            }
            case INT_64: {
              long value = (Long) record.get(name);
              double d = (double) value;
              Data.storeNum(colNum, rowNum, d);
              break;
            }
            case UINT_8: {
              long value = (Long) record.get(name);
              double d = (double) value;
              Data.storeNum(colNum, rowNum, d);
              break;
            }
            case UINT_16: {
              long value = (Long) record.get(name);
              double d = (double) value;
              Data.storeNum(colNum, rowNum, d);
              break;
            }
            case UINT_32: {
              long value = (Long) record.get(name);
              double d = (double) value;
              Data.storeNum(colNum, rowNum, d);
              break;
            }
            case UINT_64: {
              long value = (Long) record.get(name);
              double d = (double) value;
              Data.storeNum(colNum, rowNum, d);
              break;
            }
            default:
              // TODO: Fix the default case
              SFIToolkit.displayln("default case");
          }

        }
      }
    }
  }
}
