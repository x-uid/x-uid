package com.zacard.core.threadpool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 内部Thread Factory
 *
 * @author guoqw
 * @since 2018-04-20 15:21
 */
public class InternalThreadFactory implements ThreadFactory {

    private final AtomicInteger index = new AtomicInteger(0);

    private final String threadName;

    public InternalThreadFactory(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, threadName + "-" + index.incrementAndGet());
    }
}
