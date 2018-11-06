package com.kylebarron.stataParquet;

import com.stata.sfi.SFIToolkit;
import com.stata.sfi.Data;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.function.Predicate;

import static com.kylebarron.stataParquet.io.InputFile.nioPathToInputFile;

/*
    Example of reading writing Parquet in java without BigData tools.
    Run from Stata with:
    javacall com.kylebarron.stataParquet.DataLoad printParquet, jar(stataParquet.jar)
*/
public class DataLoad {
  private static final File progDirPathFile;

  static File getProgDirPath() { return progDirPathFile; }

  static {
    final Predicate<String> existsAndIsDir = dirPath -> {
      final File dirPathFile = new File(dirPath);
      return dirPathFile.exists() && dirPathFile.isDirectory();
    };
    String homeDirPath = System.getenv("HOME"); // user home directory
    homeDirPath = homeDirPath != null && !homeDirPath.isEmpty() && existsAndIsDir.test(homeDirPath) ? homeDirPath : ".";
    progDirPathFile = FileSystems.getDefault().getPath(homeDirPath).toFile();
  }

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
          Data.setObsTotal(4);
          Data.addVarLong("a");
        }
        int var = 1;
        SFIToolkit.displayln("Before record");
        long value = (Long) record.get("a");
        double d = (double) value;
        SFIToolkit.displayln("after getting record");
        Data.storeNum(var, i, d);
        SFIToolkit.displayln("after storing record");
      }
    }
  }
}
