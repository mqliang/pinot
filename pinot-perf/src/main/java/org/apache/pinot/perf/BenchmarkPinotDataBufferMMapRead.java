package org.apache.pinot.perf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.memory.PinotNativeOrderLBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class BenchmarkPinotDataBufferMMapRead {
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
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkPinotDataBufferMMapRead.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  static {
    PinotNativeOrderLBuffer.init();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  // Sequentially read all the integers from the file
  public void benchmarkPinotDataBufferMMapSequentialDenseRead()
      throws IOException {
    PinotNativeOrderLBuffer.pinotDataBufferMMapSequentialDenseRead();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  // Randomly read all the integers from the file
  public void benchmarkPinotDataBufferMMapRandomDenseRead()
      throws IOException {
    PinotNativeOrderLBuffer.pinotDataBufferMMapRandomDenseRead();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  // Sequentially load all the 4k size pages from disk, for each page just read the first integer.
  public void benchmarkPinotDataBufferMMapSequentialSparseRead()
      throws IOException {
    PinotNativeOrderLBuffer.pinotDataBufferMMapSequentialSparseRead();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  // Randomly load all the 4k size pages from disk, for each page just read the first integer.
  public void benchmarkPinotDataBufferMMapRandomSparseRead()
      throws IOException {
    PinotNativeOrderLBuffer.pinotDataBufferMMapRandomSparseRead();
  }
}
