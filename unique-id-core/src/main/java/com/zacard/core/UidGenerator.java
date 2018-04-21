package com.zacard.core;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 唯一id生成器
 * <p>
 * 借鉴Snowflake算法改进优化
 *
 * @author guoqw
 * @since 2018-04-16 21:18
 */
public class UidGenerator {

    private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

    private static final IdWorker ID_WORKER = new IdWorker();

    /**
     * 序列号所占的位数
     */
    private static final long SEQUENCE_BITS = 12L;

    /**
     * 最大序列号
     */
    public static final int MAX_SEQUENCE_COUNT = 1 << SEQUENCE_BITS;


    public static long generateId() {
        return ID_WORKER.nextId();
    }

    public static long[] generateIds(int count) {
        return ID_WORKER.batchNextIds(count);
    }

    /**
     * 判断id_worker是否busy
     *
     * @param busyFlag busy的参考基准
     */
    public static boolean isBusy(int busyFlag) {
        return ID_WORKER.lock.getQueueLength() > Math.min(busyFlag, PROCESSORS + 1);
    }

    /**
     * SnowFlake的结构如下(每部分用-分开):<br>
     * <p>
     * +------+----------------------+----------------+-----------+
     * | sign |     timestamp        | worker node id | sequence  |
     * +------+----------------------+----------------+-----------+
     * 1bit          41bits              10bits         12bits
     * <p>
     * <br>
     * 0 - 0000000000 0000000000 0000000000 0000000000 0 - 0000000000 - 000000000000 <br>
     * <br>
     * (1) 1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0<br>
     * <br>
     * (2) 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截)
     * 得到的值），这里的的开始时间截，一般是我们的id生成器开始使用的时间，由我们程序来指定的（如下`程序IdWorker类的startTime属性）。
     * 41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69<br>
     * <br>
     * (3) 10位的机器节点标示，可以部署在1<<10=1024个节点
     * <br>
     * (4) 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号<br>
     * <br>
     * <br>
     * 加起来刚好64位，为一个long型。<br>
     */
    private static class IdWorker {

        /**
         * 相对于2018-04-20 00:00:00的时间戳
         */
        private static final long START_TIME = 61484889600000L;

        /**
         * 时间戳位数
         */
        private static final long TIMESTAMP_BITS = 41;

        /**
         * 相对与START_TIME的时间戳最大大小
         */
        private static final long MAX_TIMESTAMP = ~(-1L << TIMESTAMP_BITS);

        /**
         * 工作节点标示id所占的位数
         */
        private static final long WORKER_ID_BITS = 10L;

        /**
         * 最大工作节点的数量
         */
        private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

        /**
         * 工作节点标示id位移数
         */
        private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;

        /**
         * 时间戳位移数
         */
        private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

        /**
         * 就是用来对sequence做快速取模操作的
         */
        private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

        /**
         * 工作节点标示id
         */
        private long workerId;

        /**
         * 序列号
         */
        private long sequence = -1L;

        /**
         * 上次时间戳
         */
        private volatile long lastTimestamp = -1L;

        /**
         * 默认最大容忍回拨时间15毫秒
         */
        private static final long DEFAULT_MAX_TOLERATE_CALLBACK_TIME = 15;

        /**
         * 最大容忍回拨的毫秒时间
         */
        private long maxTolerateCallbackTime = DEFAULT_MAX_TOLERATE_CALLBACK_TIME;

        private final ReentrantLock lock;

        private IdWorker() {
            this(reloadWorkerId());
        }

        private IdWorker(long workerId) {
            if (workerId < 0 || workerId > MAX_WORKER_ID) {
                throw new IllegalArgumentException("worker id过大,不能超过" + MAX_WORKER_ID);
            }
            this.workerId = workerId;
            this.lock = new ReentrantLock();
        }

        public long nextId() {
            return batchNextIds(1)[0];
        }

        public long[] batchNextIds(int count) {
            if (count > MAX_SEQUENCE_COUNT) {
                throw new RuntimeException("批量获取唯一ID的数量不能超过" + MAX_SEQUENCE_COUNT + "个");
            }
            lock.lock();
            try {
                long timestamp;
                long diff;

                // 这里应该是时间回拨或者人工调整了时间
                while ((diff = (timestamp = timeGen()) - lastTimestamp) < 0) {
                    /*
                     * 2个策略：
                     *      (1) 如果回拨的时间小于maxTolerateCallbackTime(默认15毫秒),则等待时间赶上lastTimestamp
                     *      (2) 否则重新获取一个更大的workerId并且重置lastTimestamp
                     */
                    if (diff <= -maxTolerateCallbackTime) {
                        waitForDelay(diff);
                    } else {
                        workerId = reloadWorkerId();
                        lastTimestamp = timestamp;
                        sequence = -1L;
                        return generateIds(timestamp, workerId, count);
                    }
                }
                // 时间戳改变，毫秒内序列重置
                if (diff > 0) {
                    sequence = -1L;
                }
                return generateIds(timestamp, workerId, count);
            } finally {
                lock.unlock();
            }
        }

        private long[] generateIds(long timestamp, long workerId, int count) {
            // 超过了时间戳位数的情况
            if (((sequence + count) >> SEQUENCE_BITS) + timestamp > MAX_TIMESTAMP) {
                throw new RuntimeException("设置的时间戳位数已经使用到上限");
            }
            long[] ids = new long[count];
            for (int i = 0; i < count; i++) {
                // 同一毫秒内，进行序列自增
                if ((++sequence & SEQUENCE_MASK) == 0) {
                    // 数字溢出了，等待下一个毫秒
                    lastTimestamp = timestamp = tilNextMillis(lastTimestamp);
                }
                ids[i] = (workerId << WORKER_ID_SHIFT) | ((timestamp - START_TIME) << TIMESTAMP_SHIFT) | sequence;
            }
            return ids;
        }

        /**
         * 等待到下一个毫秒时间戳
         */
        private long tilNextMillis(long lastTimestamp) {
            long timestamp = timeGen();
            while (timestamp <= lastTimestamp) {
                timestamp = timeGen();
            }
            return timestamp;
        }

        /**
         * 等待指定的时间
         */
        private void waitForDelay(long delayMs) {
            /*
             * window系统sleep时间必须是10的整数
             * 参考：https://github.com/netty/netty/issues/356
             */
            boolean isWindows = System.getProperty("os.name", "").toLowerCase(Locale.US).contains("win");
            if (isWindows) {
                delayMs = delayMs / 10 * 10;
            }
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException ignore) {
                // ignore
            }
        }

        /**
         * 当前时间戳
         */
        private long timeGen() {
            return System.currentTimeMillis();
        }

        /**
         * 重新加载workerId
         */
        private static long reloadWorkerId() {
            // 0.先检查本地文件中的workerId和lastTimeStamp
            // TODO 使用spi的方式，加载load workerid的类，默认弱依赖zk
            return justForTestWorkerId.incrementAndGet();
        }

        private static final AtomicInteger justForTestWorkerId = new AtomicInteger(0);
    }

    public static void main(String[] args) {
/*        for (int i = 0; i < 10; i++) {
            System.out.println(UidGenerator.generateId());
        }*/
        long[] ids = UidGenerator.generateIds(100);
        for (int i = 0; i < ids.length; i++) {
            System.out.println(ids[i]);
        }
    }

}
