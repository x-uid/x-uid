package com.zacard.benchmark;

/**
 * @author guoqw
 * @since 2018-04-19 15:10
 */
public class RawSnowFlake {

    /**
     * 相对于2018-04-20 00:00:00的时间戳
     */
    private static final long START_TIME = 1524153600000L;

    /**
     * 时间戳位数
     */
    private static final long TIMESTAMP_BITS = 41;

    /**
     * 相对于START_TIME的时间戳最大大小
     */
    private static final long MAX_TIMESTAMP = START_TIME + ~(-1L << TIMESTAMP_BITS);

    /**
     * 工作节点标示id所占的位数
     */
    private static final long WORKER_ID_BITS = 10L;

    /**
     * 最大工作节点的数量
     */
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    /**
     * 序列号所占的位数
     */
    private static final long SEQUENCE_BITS = 12L;

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
    private long sequence = 0L;

    /**
     * 上次时间戳
     */
    private long lastTimestamp = -1L;


    public RawSnowFlake() {
        this(0);
    }

    private RawSnowFlake(long workerId) {
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException("worker id过大,不能超过" + MAX_WORKER_ID);
        }
        this.workerId = workerId;
    }

    public synchronized long nextId() {
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            throw new RuntimeException("时间回拨");
        }
        if (timestamp == lastTimestamp) {
            // 同一毫秒内，进行序列自增
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                // 数字溢出了，等待下一个毫秒
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            // 时间戳改变，毫秒内序列重置
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        return generateId(timestamp, workerId, sequence);
    }

    /**
     * 组装id
     */
    private long generateId(long timestamp, long workerId, long sequence) {
        // 超过了时间戳位数的情况
        if (timestamp > MAX_TIMESTAMP) {
            throw new RuntimeException("设置的时间戳位数已经使用到上限");
        }
        return ((timestamp - START_TIME) << TIMESTAMP_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
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
     * 当前时间戳
     */
    private long timeGen() {
        return System.currentTimeMillis();
    }

}
