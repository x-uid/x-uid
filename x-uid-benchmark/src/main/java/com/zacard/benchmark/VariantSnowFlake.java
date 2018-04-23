package com.zacard.benchmark;

import com.zacard.core.cache.IdCacheWorker;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/**
 * @author guoqw
 * @since 2018-04-19 15:32
 */
@State(value = Scope.Benchmark)
public class VariantSnowFlake {

    private IdCacheWorker idCacheWorker = new IdCacheWorker();

    @Setup
    public void setUp() {
        System.out.println("cpu核数："+Runtime.getRuntime().availableProcessors());
        System.out.println("开始变种版本的SnowFlake的微基准测试.");
    }

    @Threads(value = Threads.MAX)
    @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.MILLISECONDS)
    @Fork(value = 5)
    @Benchmark
    @BenchmarkMode(value = Mode.Throughput)
    @OutputTimeUnit(value = TimeUnit.SECONDS)
    public void benchMarkCollector() {
        idCacheWorker.nextId();
    }
}
