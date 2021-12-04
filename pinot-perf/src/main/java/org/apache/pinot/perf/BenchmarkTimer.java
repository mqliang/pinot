package org.apache.pinot.perf;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.query.request.context.ThreadTimer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 30)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkTimer {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkTimer.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Benchmark
  public void benchmarkThreadCpuTimer() {
    ThreadTimer threadTimer = new ThreadTimer();
    long totalThreadCpuTimeNs = threadTimer.getThreadTimeNs();
  }

  @Benchmark
  public void benchmarkSystemCurrentTimeMillis() {
    long startWallClockTimeMs = System.currentTimeMillis();
    long totalWallClockTimeMs = System.currentTimeMillis() - startWallClockTimeMs;
    long totalWallClockTimeNs = TimeUnit.MILLISECONDS.toNanos(totalWallClockTimeMs);
  }

  @Benchmark
  public void benchmarkSystemNanoTime() {
    long startWallClockTimeNs = System.nanoTime();
    long totalWallClockTimeNs = System.nanoTime() - startWallClockTimeNs;
  }
}
