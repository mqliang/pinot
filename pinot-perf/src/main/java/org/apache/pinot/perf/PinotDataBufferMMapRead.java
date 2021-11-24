package org.apache.pinot.perf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotNativeOrderLBuffer;
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
import org.testng.Assert;


@State(Scope.Benchmark)
public class PinotDataBufferMMapRead {
  private static final String RESOURCES_PATH =
      Paths.get(".").toAbsolutePath().toString() + "/pinot-perf/src/main/resources/";
  private static final File TEST_FILE = new File(RESOURCES_PATH + "PinotDataBufferTest.txt");
  private static final long FILE_SIZE_GB = 125;
  private static final int VALUE = Integer.MAX_VALUE;
  private static final Random RANDOM = new Random();
  private static final long FILE_SIZE_BYTES = FILE_SIZE_GB * 1024 * 1024 * 1024;
  private static final long PAGE_SIZE_BYTES = 4 * 1024;
  private static final long PAGE_NUM = FILE_SIZE_BYTES / PAGE_SIZE_BYTES;

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(PinotDataBufferMMapRead.class.getSimpleName()).warmupTime(TimeValue.minutes(0))
            .warmupIterations(0).measurementTime(TimeValue.minutes(30)).measurementIterations(5).forks(1);
    new Runner(opt.build()).run();
  }

  // Create a file with given size on disk, and fill the file with Integer.MAX_VALUE
  // NOTE: not use "randomAccessFile.setLength()" --> JVM/OS will create a sparse file
  static {
    if (!TEST_FILE.exists() || TEST_FILE.length() != FILE_SIZE_BYTES) {
      try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
        long offset = 0;
        while (offset != FILE_SIZE_BYTES) {
          buffer.putInt(offset, VALUE);
          offset += Integer.BYTES;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    Assert.assertEquals(FILE_SIZE_BYTES, TEST_FILE.length());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  // Sequentially read all the integers from the file
  public void BenchmarkPinotDataBufferMMapSequentialDenseRead()
      throws IOException {
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
      long totalNums = FILE_SIZE_BYTES / Integer.BYTES;
      for (long i = 0; i < totalNums; i++) {
        long offset = i * Integer.BYTES;
        int actualValue = buffer.getInt(offset);
        // Just in case of JVM optimizing `buffer.getInt(offset);`
        Assert.assertEquals(actualValue, VALUE);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  // Randomly read all the integers from the file
  public void BenchmarkPinotDataBufferMMapRandomDenseRead()
      throws IOException {
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
      long totalNums = FILE_SIZE_BYTES / Integer.BYTES;
      for (long i = 0; i < totalNums; i++) {
        long offset = (long) (RANDOM.nextFloat() * totalNums) * Integer.BYTES ;
        int actualValue = buffer.getInt(offset);
        // Just in case of JVM optimizing `buffer.getInt(offset);`
        Assert.assertEquals(actualValue, VALUE);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  // Sequentially load all the 4k size pages from disk, for each page just read the first integer.
  public void BenchmarkPinotDataBufferMMapSequentialSparseRead()
      throws IOException {
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
      for (long i = 0; i < PAGE_NUM; i++) {
        long offset = i * PAGE_SIZE_BYTES;
        int actualValue = buffer.getInt(offset);
        // Just in case of JVM optimizing `buffer.getInt(offset);`
        Assert.assertEquals(actualValue, VALUE);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  // Randomly load all the 4k size pages from disk, for each page just read the first integer.
  public void BenchmarkPinotDataBufferMMapRandomSparseRead()
      throws IOException {
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
      for (int i = 0; i < PAGE_NUM; i++) {
        long offset = (long) (RANDOM.nextFloat() * PAGE_NUM) * PAGE_SIZE_BYTES ;
        int actualValue = buffer.getInt(offset);
        // Just in case of JVM optimizing `buffer.getInt(offset);`
        Assert.assertEquals(actualValue, VALUE);
      }
    }
  }
}
