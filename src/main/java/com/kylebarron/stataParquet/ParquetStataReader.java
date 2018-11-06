package com.kylebarron.stataParquet;

import com.stata.sfi.Data;
import com.stata.sfi.SFIToolkit;

import org.apache.hadoop.conf.Configuration;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

import java.util.ArrayList;
import java.util.List;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import javax.annotation.Nonnull;

import static com.kylebarron.stataParquet.io.InputFile.nioPathToInputFile;


/**
 * Read Parquet file into Stata
 *
 * @author Kyle Barron
 * @version 0.0.0
 * javacall com.kylebarron.stataParquet.ParquetStataReader read, jar(stataParquetShaded.jar)
 */
public class ParquetStataReader {

  public static int read(String[] args) {
    try {
      String filePath = args[0];
      ParquetMetadata metadata = getMetadata(filePath);

      // Set number of observations in data
      setStataObs(metadata);

      // Store data in Stata
      final Path parquetFilePath = FileSystems.getDefault().getPath(filePath);
      readFromParquet(parquetFilePath);

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

  private static void setStataObs(ParquetMetadata metadata) {
    long num_rows = getRows(metadata);
    Data.setObsTotal(num_rows);
  }

  private static void readFromParquet(@Nonnull final Path filePathToRead) throws IOException {
    try (final ParquetReader<GenericData.Record> reader = AvroParquetReader
            .<GenericData.Record>builder(nioPathToInputFile(filePathToRead))
            .withConf(new Configuration())
            .build())
    {

      // Read first row separately
      GenericData.Record firstRecord = reader.read();
      Schema schema = firstRecord.getSchema();
      List<Type> types = createStataColumns(schema);
      long rowNum = 1;
      fillData(firstRecord, types, rowNum);

      GenericData.Record record;
      while ((record = reader.read()) != null) {
        rowNum += 1;
        fillData(record, types, rowNum);
      }
    }
  }

  private static List<Type> createStataColumns(Schema schema) {
    List<Type> types = new ArrayList<Type>();
    List<Field> fields = schema.getFields();

    for (Field field: fields) {

      String name = field.name();
      Schema fieldSchema = field.schema();
      Type type = fieldSchema.getType();

      switch (type) {
        case STRING: {
          Data.addVarStr(name, 1);
          types.add(Type.STRING);
          break;
        }
        case INT: {
          Data.addVarInt(name);
          types.add(Type.INT);
          break;
        }
        case LONG: {
          Data.addVarDouble(name);
          types.add(Type.LONG);
          break;
        }
        case FLOAT: {
          Data.addVarFloat(name);
          types.add(Type.FLOAT);
          break;
        }
        case DOUBLE: {
          Data.addVarDouble(name);
          types.add(Type.DOUBLE);
          break;
        }
        case BOOLEAN: {
          Data.addVarByte(name);
          types.add(Type.BOOLEAN);
          break;
        }
        case UNION: {
          List<Schema> unionSchema = fieldSchema.getTypes();
          List<Type> unionTypes = new ArrayList<Type>();
          unionSchema.forEach((innerSchema) -> unionTypes.add(innerSchema.getType()));
          if (unionTypes.contains(Type.STRING)) {
            Data.addVarStr(name, 1);
            types.add(Type.STRING);
          } else if (unionTypes.contains(Type.INT)) {
            Data.addVarInt(name);
            types.add(Type.INT);
          } else if (unionTypes.contains(Type.LONG)) {
            Data.addVarDouble(name);
            types.add(Type.LONG);
          } else if (unionTypes.contains(Type.FLOAT)) {
            Data.addVarFloat(name);
            types.add(Type.FLOAT);
          } else if (unionTypes.contains(Type.DOUBLE)) {
            Data.addVarDouble(name);
            types.add(Type.DOUBLE);
          } else if (unionTypes.contains(Type.BOOLEAN)) {
            Data.addVarByte(name);
            types.add(Type.BOOLEAN);
          }
          break;
        }
        default: {
          SFIToolkit.displayln("Don't know what to do with var: " + name + ", type: " + type.toString());
          types.add(Type.NULL);
          break;
        }
      }
    }
    return types;
  }

  private static void fillData(GenericData.Record record, List<Type> types, long rowNum) {

    int colNum = -1;
    for (Type colType: types) {
      // Avro data structures are zero-indexed, Stata is one-indexed
      colNum += 1;

      switch (colType) {
        case STRING: {
          String value = record.get(colNum).toString();
          Data.storeStr(colNum + 1, rowNum, value);
          break;
        }
        case INT: {
          long value = (long) record.get(colNum);
          double d = (double) value;
          Data.storeNum(colNum + 1, rowNum, d);
          break;
        }
        case LONG: {
          long value = (long) record.get(colNum);
          double d = (double) value;
          Data.storeNum(colNum + 1, rowNum, d);
          break;
        }
        case FLOAT: {
          double value = (double) record.get(colNum);
          Data.storeNum(colNum + 1, rowNum, value);
          break;
        }
        case DOUBLE: {
          double value = (double) record.get(colNum);
          Data.storeNum(colNum + 1, rowNum, value);
          break;
        }
        case BOOLEAN: {
          boolean value = (boolean) record.get(colNum);
          if (value == true) {
            Data.storeNum(colNum + 1, rowNum, 1);
          } else {
            Data.storeNum(colNum + 1, rowNum, 0);
          }
          break;
        }
        default: {
          break;
        }
      }
    }
  }
}
