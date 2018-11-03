package com.kylebarron.stataParquet;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.function.Predicate;

import static com.kylebarron.stataParquet.io.InputFile.nioPathToInputFile;

/*
    Example of reading writing Parquet in java without BigData tools.
*/
public class DataLoad {
  private static final Logger LOGGER;
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
    LoggingLevel.setLoggingVerbosity(LoggingLevel.DEBUG);
    LOGGER = LoggingLevel.effectLoggingLevel(() -> LoggerFactory.getLogger(DataLoad.class.getSimpleName()));
  }

  public static void main(String[] args) {
    try {
      final Path parquetFilePath = FileSystems.getDefault().getPath("sample.parquet");
      doTestParquet(parquetFilePath);
    } catch (Throwable e) {
      LOGGER.error("program terminated due to exception:", e);
      System.exit(1); // return non-zero status to indicate program failure
    }
  }

  private static void doTestParquet(final Path parquetFilePath)
          throws IOException
  {
    readFromParquet(parquetFilePath);
  }

  private static void readFromParquet(@Nonnull final Path filePathToRead) throws IOException {
    try (final ParquetReader<GenericData.Record> reader = AvroParquetReader
            .<GenericData.Record>builder(nioPathToInputFile(filePathToRead))
            .withConf(new Configuration())
            .build())
    {
      GenericData.Record record;
      while ((record = reader.read()) != null) {
        System.out.println(record);
      }
    }
  }
}
