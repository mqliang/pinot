package org.apache.pinot.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.spi.utils.ByteArray;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
public class BenchmarkDataTableBulkBuild {

  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final int NUM_ROWS = 100;

  static int[] ints = new int[NUM_ROWS];
  static long[] longs = new long[NUM_ROWS];
  static float[] floats = new float[NUM_ROWS];
  static double[] doubles = new double[NUM_ROWS];
  static String[] strings = new String[NUM_ROWS];
  static byte[][] bytes = new byte[NUM_ROWS][];
  static Object[] objects = new Object[NUM_ROWS];
  static int[][] intArrays = new int[NUM_ROWS][];
  static long[][] longArrays = new long[NUM_ROWS][];
  static float[][] floatArrays = new float[NUM_ROWS][];
  static double[][] doubleArrays = new double[NUM_ROWS][];
  static String[][] stringArrays = new String[NUM_ROWS][];

  static DataSchema.ColumnDataType[] columnDataTypes = DataSchema.ColumnDataType.values();
  static int numColumns = columnDataTypes.length;
  static String[] columnNames = new String[numColumns];

  // generate random data.
  static {
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = columnDataTypes[i].name();
    }
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      for (int colId = 0; colId < numColumns; colId++) {
        switch (columnDataTypes[colId]) {
          case INT:
            ints[rowId] = RANDOM.nextInt();
            break;
          case LONG:
            longs[rowId] = RANDOM.nextLong();
            break;
          case FLOAT:
            floats[rowId] = RANDOM.nextFloat();
            break;
          case DOUBLE:
            doubles[rowId] = RANDOM.nextDouble();
            break;
          case STRING:
            strings[rowId] = RandomStringUtils.random(RANDOM.nextInt(20));
            break;
          case BYTES:
            bytes[rowId] = RandomStringUtils.random(RANDOM.nextInt(20)).getBytes();
            break;
          case OBJECT:
            objects[rowId] = RANDOM.nextDouble();
            break;
          case INT_ARRAY:
            int length = RANDOM.nextInt(20);
            int[] intArray = new int[length];
            for (int i = 0; i < length; i++) {
              intArray[i] = RANDOM.nextInt();
            }
            intArrays[rowId] = intArray;
            break;
          case LONG_ARRAY:
            length = RANDOM.nextInt(20);
            long[] longArray = new long[length];
            for (int i = 0; i < length; i++) {
              longArray[i] = RANDOM.nextLong();
            }
            longArrays[rowId] = longArray;
            break;
          case FLOAT_ARRAY:
            length = RANDOM.nextInt(20);
            float[] floatArray = new float[length];
            for (int i = 0; i < length; i++) {
              floatArray[i] = RANDOM.nextFloat();
            }
            floatArrays[rowId] = floatArray;
            break;
          case DOUBLE_ARRAY:
            length = RANDOM.nextInt(20);
            double[] doubleArray = new double[length];
            for (int i = 0; i < length; i++) {
              doubleArray[i] = RANDOM.nextDouble();
            }
            doubleArrays[rowId] = doubleArray;
            break;
          case STRING_ARRAY:
            length = RANDOM.nextInt(20);
            String[] stringArray = new String[length];
            for (int i = 0; i < length; i++) {
              stringArray[i] = RandomStringUtils.random(RANDOM.nextInt(20));
            }
            stringArrays[rowId] = stringArray;
            break;
        }
      }
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkDataTableBulkBuild.class.getSimpleName())
        .warmupTime(TimeValue.seconds(10)).warmupIterations(1).measurementTime(TimeValue.seconds(30))
        .measurementIterations(5).forks(1);
    new Runner(opt.build()).run();
  }

  // Benchmark data table values filling code of: for each row, set value for each column (a lot of
  // ByteBuffer.position() calling). Set value of columns in order of: 1st col, 2nd col, 3nd col...
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public DataTable BenchmarkDataTableRowIdColIdBuildInOrder()
      throws IOException {
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      dataTableBuilder.startRow();
      for (int colId = 0; colId < numColumns; colId++) {
        switch (columnDataTypes[colId]) {
          case INT:
            dataTableBuilder.setColumn(colId, ints[rowId]);
            break;
          case LONG:
            dataTableBuilder.setColumn(colId, longs[rowId]);
            break;
          case FLOAT:
            dataTableBuilder.setColumn(colId, floats[rowId]);
            break;
          case DOUBLE:
            dataTableBuilder.setColumn(colId, doubles[rowId]);
            break;
          case STRING:
            dataTableBuilder.setColumn(colId, strings[rowId]);
            break;
          case BYTES:
            dataTableBuilder.setColumn(colId, new ByteArray(bytes[rowId]));
            break;
          case OBJECT:
            dataTableBuilder.setColumn(colId, objects[rowId]);
            break;
          case INT_ARRAY:
            dataTableBuilder.setColumn(colId, intArrays[rowId]);
            break;
          case LONG_ARRAY:
            dataTableBuilder.setColumn(colId, longArrays[rowId]);
            break;
          case FLOAT_ARRAY:
            dataTableBuilder.setColumn(colId, floatArrays[rowId]);
            break;
          case DOUBLE_ARRAY:
            dataTableBuilder.setColumn(colId, doubleArrays[rowId]);
            break;
          case STRING_ARRAY:
            dataTableBuilder.setColumn(colId, stringArrays[rowId]);
            break;
        }
      }
      dataTableBuilder.finishRow();
    }
    return dataTableBuilder.build();
  }

  // Benchmark data table values filling code of: for each row, set value for each column (a lot of
  // ByteBuffer.position() calling). Set value of columns in random order.
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public DataTable BenchmarkDataTableRowIdColIdBuildRandomOrder()
      throws IOException {
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);

    List<Integer> colIds = new ArrayList<>(numColumns);
    for (int i = 0; i < numColumns; i++) {
      colIds.add(i);
    }
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      Collections.shuffle(colIds);
      dataTableBuilder.startRow();
      for (int colId : colIds) {
        switch (columnDataTypes[colId]) {
          case INT:
            dataTableBuilder.setColumn(colId, ints[rowId]);
            break;
          case LONG:
            dataTableBuilder.setColumn(colId, longs[rowId]);
            break;
          case FLOAT:
            dataTableBuilder.setColumn(colId, floats[rowId]);
            break;
          case DOUBLE:
            dataTableBuilder.setColumn(colId, doubles[rowId]);
            break;
          case STRING:
            dataTableBuilder.setColumn(colId, strings[rowId]);
            break;
          case BYTES:
            dataTableBuilder.setColumn(colId, new ByteArray(bytes[rowId]));
            break;
          case OBJECT:
            dataTableBuilder.setColumn(colId, objects[rowId]);
            break;
          case INT_ARRAY:
            dataTableBuilder.setColumn(colId, intArrays[rowId]);
            break;
          case LONG_ARRAY:
            dataTableBuilder.setColumn(colId, longArrays[rowId]);
            break;
          case FLOAT_ARRAY:
            dataTableBuilder.setColumn(colId, floatArrays[rowId]);
            break;
          case DOUBLE_ARRAY:
            dataTableBuilder.setColumn(colId, doubleArrays[rowId]);
            break;
          case STRING_ARRAY:
            dataTableBuilder.setColumn(colId, stringArrays[rowId]);
            break;
        }
      }
      dataTableBuilder.finishRow();
    }
    return dataTableBuilder.build();
  }

  // Benchmark data table values filling code of: for each row, set value for the row in bulk (no
  // ByteBuffer.position() calling).
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public DataTable BenchmarkDataTableRowBulkBuild()
      throws IOException {
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      dataTableBuilder.startRow();
      Object[] colValues = new Object[numColumns];
      for (int colId = 0; colId < numColumns; colId++) {
        switch (columnDataTypes[colId]) {
          case INT:
            colValues[colId] = ints[rowId];
            break;
          case LONG:
            colValues[colId] = longs[rowId];
            break;
          case FLOAT:
            colValues[colId] = floats[rowId];
            break;
          case DOUBLE:
            colValues[colId] = doubles[rowId];
            break;
          case STRING:
            colValues[colId] = strings[rowId];
            break;
          case BYTES:
            colValues[colId] = bytes[rowId];
            break;
          case OBJECT:
            colValues[colId] = objects[rowId];
            break;
          case INT_ARRAY:
            colValues[colId] = intArrays[rowId];
            break;
          case LONG_ARRAY:
            colValues[colId] = longArrays[rowId];
            break;
          case FLOAT_ARRAY:
            colValues[colId] = floatArrays[rowId];
            break;
          case DOUBLE_ARRAY:
            colValues[colId] = doubleArrays[rowId];
            break;
          case STRING_ARRAY:
            colValues[colId] = stringArrays[rowId];
            break;
        }
      }
      dataTableBuilder.setColumnValuesInBulk(columnDataTypes, colValues);
      dataTableBuilder.finishRow();
    }
    return dataTableBuilder.build();
  }
}