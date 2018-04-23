package com.zacard.benchmark;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/**
 * @author guoqw
 * @since 2018-04-19 15:16
 */
@State(value = Scope.Benchmark)
public class RawSnowFlakeBenchmark {

    private RawSnowFlake rawSnowFlake = new RawSnowFlake();

    @Setup
    public void setUp() {
        System.out.println("开始原始版本的SnowFlake的微基准测试.");
    }

    @Threads(value = Threads.MAX)
    @Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.MILLISECONDS)
    @Fork(value = 5)
    @Benchmark
    @BenchmarkMode(value = Mode.Throughput)
    @OutputTimeUnit(value = TimeUnit.SECONDS)
    public void benchMarkCollector() {
        // 4019761.053, 4256134.611, 4449444.820
        rawSnowFlake.nextId();
    }
}
