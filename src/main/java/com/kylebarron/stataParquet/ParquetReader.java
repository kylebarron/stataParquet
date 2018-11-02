package com.kylebarron.stataParquet;

import com.stata.sfi.*;

/**
 * Read Parquet file into Stata
 *
 * @author Kyle Barron
 * @version 0.0.0
 */
public class ParquetReader {

    /**
     * Boiler plate method that you can use quickly to get up and running.
     * @param args This value is automatically obtained from the args()
     *                option of the javacall command.  If possible, writting
     *                your plugin to pass the parameters to this method will
     *                make it easier to test/debug the Java side of things
     *                independent of Stata.  To do that change 'int
     *                entryPoint' to 'void main', recompile, and then you
     *                should be able to run the same plugin from the command
     *                line.
     * @return The method must return an integer value to conform to the Java
     * API expectations.  Non-zero values trigger errors (much the same way
     * that non-zero returns in other compiled languages often indicate errors)
     */
    public static int methodUsedByJavacallCommand(String[] args) {

        return 0;

    }

    public static int sayhello(String[] args)
    {
        SFIToolkit.displayln("Hello from java!") ;
        return(0) ;      // Stata return code
    }

}
//
// public class Main {
//
//     private static Path path = new Path("file:\\C:\\Users\\file.snappy.parquet");
//
//     private static void printGroup(Group g) {
//         int fieldCount = g.getType().getFieldCount();
//         for (int field = 0; field < fieldCount; field++) {
//             int valueCount = g.getFieldRepetitionCount(field);
//
//             Type fieldType = g.getType().getType(field);
//             String fieldName = fieldType.getName();
//
//             for (int index = 0; index < valueCount; index++) {
//                 if (fieldType.isPrimitive()) {
//                     System.out.println(fieldName + " " + g.getValueToString(field, index));
//                 }
//             }
//         }
//
//     }
//
//     public static void main(String[] args) throws IllegalArgumentException {
//
//         Configuration conf = new Configuration();
//
//         try {
//             ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
//             MessageType schema = readFooter.getFileMetaData().getSchema();
//             ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);
//
//             PageReadStore pages = null;
//             try {
//                 while (null != (pages = r.readNextRowGroup())) {
//                     final long rows = pages.getRowCount();
//                     System.out.println("Number of rows: " + rows);
//
//                     final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
//                     final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
//                     for (int i = 0; i < rows; i++) {
//                         final Group g = recordReader.read();
//                         printGroup(g);
//
//                         // TODO Compare to System.out.println(g);
//                     }
//                 }
//             } finally {
//                 r.close();
//             }
//         } catch (IOException e) {
//             System.out.println("Error reading parquet file.");
//             e.printStackTrace();
//         }
//
//     }
// }