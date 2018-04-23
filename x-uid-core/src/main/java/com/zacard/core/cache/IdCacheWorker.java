package com.zacard.core.cache;

import com.zacard.core.UidGenerator;
import com.zacard.core.file.UidPersistence;
import com.zacard.core.queue.DistinctLinkedBlockingQueue;
import com.zacard.core.threadpool.InternalThreadFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.zacard.core.UidGenerator.MAX_SEQUENCE_COUNT;

/**
 * 缓存部分唯一id
 *
 * @author guoqw
 * @since 2018-04-18 10:27
 */
public class IdCacheWorker {

    /**
     * buffer最大容量
     */
    private static final int MAXIMUM_SIZE = 1 << 30;

    /**
     * 默认的缓存区id个数:1<<12=4096
     */
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    /**
     * 初始化容量
     */
    private int initSize;

    /**
     * 实际容量，肯定为2的n次方
     */
    private int threshold;

    /**
     * 用来快速取模
     */
    private int thresholdMask;

    /**
     * 默认逻辑分区数量
     */
    private static final int DEFAULT_PARTITION_NUMBER = 4;

    /**
     * 逻辑分区数量
     */
    private int partitionNumber = DEFAULT_PARTITION_NUMBER;

    /**
     * 对于缓冲区的每个逻辑分区size,默认分4个分区
     */
    private int partitionSize;

    /**
     * 用于快速分区取模
     */
    private int partitionSizeMask;

    /**
     * 缓存数组,一级缓存
     */
    private IdHolder[] buffer;

    /**
     * 当前消费的位置
     */
    private AtomicInteger rangeIndex = new AtomicInteger(0);

    /**
     * 线程池
     */
    private ExecutorService threadPool;

    /**
     * 线程池中最大线程数
     */
//    private int maxThreads = 4;

    /**
     * 普通nextId重试次数
     */
    private int maxRetry = 6;

    /**
     * 线程池队列任务数量
     */
    private int threadPoolQueueSize = 128;

    /**
     * 二级缓存, 数组id缓存队列, 每个元素存放批量获取的long型的id数组
     */
//    private ConcurrentLinkedQueue<long[]> cacheArrayIds;
    private ArrayBlockingQueue<long[]> cacheArrayIds;

    /**
     * cacheArrayIds队列的最大容量
     */
    private static final int MAX_CACHE_ARRAY_QUEUE_SIZE = 1 << 7;

    /**
     * 二级缓存, 单id缓存队列, 每个元素存放单个获取的long型的id值
     */
//    private ConcurrentLinkedQueue<Long> cacheSingleIds;
    private ArrayBlockingQueue<Long> cacheSingleIds;

    /**
     * cacheSingleIds队列的最大容量
     */
    private static final int MAX_CACHE_SINGLE_QUEUE_SIZE = 1 << 20;

    /**
     * 启动初始化花费几秒写入di到文件
     * 1s ≈ 4096000个id
     */
    private static final int INIT_FILE_SECOND = 5;

    /**
     * uid持久化到文件服务
     */
    private UidPersistence uidPersistence;

    /**
     * 判断变种snowflake的生成id是否繁忙的参考值
     * <p>
     * 等待的线程数
     */
    private int busyFlag = 1;

    public IdCacheWorker() {
        this(DEFAULT_BUFFER_SIZE);
    }

    public IdCacheWorker(int initSize) {
        if (initSize < 0) {
            throw new IllegalArgumentException("Illegal initial capacity: " + initSize);
        }
        if (initSize > MAXIMUM_SIZE) {
            initSize = MAXIMUM_SIZE;
        }
        this.initSize = initSize;
        this.threshold = tableSizeFor(initSize);
        this.thresholdMask = threshold - 1;
        this.buffer = new IdHolder[threshold];
        // 这里必须保证能整除
        this.partitionSize = threshold / partitionNumber;
        this.partitionSizeMask = partitionSize - 1;

        // size大小控制在 MAX_CACHE_ARRAY_QUEUE_SIZE 范围内
        this.cacheArrayIds = new ArrayBlockingQueue<>(MAX_CACHE_ARRAY_QUEUE_SIZE);
        // size大小控制在 MAX_CACHE_SINGLE_QUEUE_SIZE 范围内
//        this.cacheSingleIds = new ConcurrentLinkedQueue<>();
        this.cacheSingleIds = new ArrayBlockingQueue<>(MAX_CACHE_SINGLE_QUEUE_SIZE);

        // id文件持久化初始化
        this.uidPersistence = new UidPersistence();

        // 初始化填充缓存区
        fillBuffer();
        // 初始化单id缓存队列
        fillCache();
        // 初始化填充文件数据
        initPadFile();

        // 启动定时任务
        this.scheduleJob();
        // 初始化线程池
        this.threadPool = initThreadPool();

    }

    /**
     * 填充整个缓存区
     * 这个方法应该只在启动初始化的时候调用
     * <p>
     * 因此，这个方法填充的时候没有竞争，不需要使用cas的方式填充
     */
    private void fillBuffer() {
        int start, count, num = (threshold + MAX_SEQUENCE_COUNT - 1) / MAX_SEQUENCE_COUNT;
        long[] ids;
        for (int i = 0; i < num; i++) {
            start = i * MAX_SEQUENCE_COUNT;
            count = (i == num - 1) ? threshold - start : MAX_SEQUENCE_COUNT;
            ids = nextIdRaw(count);
            for (int j = 0; j < count; j++) {
                buffer[start + j] = IdHolder.fill(ids[j]);
            }
        }
    }

    /**
     * 填充整个单id缓存
     */
    private void fillCache() {
        long num = (MAX_CACHE_SINGLE_QUEUE_SIZE - 1) / MAX_SEQUENCE_COUNT;
        long[] ids;
        for (long i = 0; i < num; i++) {
            ids = nextIdRaw(MAX_SEQUENCE_COUNT);
            cacheSingleIds.addAll(LongStream.of(ids).boxed().collect(Collectors.toList()));
        }
    }

    /**
     * 从原始加锁的方式获取下一批id
     */
    private long[] nextIdRaw(int count) {
        return UidGenerator.generateIds(count);
    }

    /**
     * 获取下一个唯一id
     * <p>
     * 这里是无锁，cas代替锁
     */
    public long nextId() {
        // 0.先尝试去缓存区中获取数据
        // 这里由于可能受randomNextId()的影响，将重试一定的次数
        int retry = maxRetry;
        while (retry-- != 0) {
            int index = nextIndex(true);
            IdHolder idHolder = getVolatile(index);
            if (idHolder != null && casObject(index, idHolder, null)) {
                // 获取id成功后，判断是否需要重新load一遍缓冲区的分区数据,异步处理
                if (((index + 1) & partitionSizeMask) == 0) {
                    addLoadTask(index);
                }
                return idHolder.getValue();
            }
        }
        // 1.被其他线程抢先捞走了这个格子的数据(只出现在整个缓存行一圈的数据都被同时取走的情况)
        // 或者受randomNextId()影响
        // 退而求其次，从单id缓存队列中获取
        Long id = null;
        try {
            id = cacheSingleIds.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 3.如果从缓存队列中获取失败,则直接从加锁方法获取
        return id == null ? nextIdRaw(1)[0] : id;
    }

    /**
     * 添加一个load id到缓存区分区的任务
     * 分区load也进一步减少了竞争
     */
    private void addLoadTask(int currentIndex) {
        try {
            threadPool.execute(new LoadIdTask(currentIndex, partitionSize, buffer, cacheArrayIds));
        } catch (Exception e) {
            // 这里可能是任务拒绝异常
            e.printStackTrace();
        }
    }

    private boolean isBusy() {
        return UidGenerator.isBusy(busyFlag);
    }

    private void scheduleJob() {
        // 空闲期cache&文件写入线程
        // 在空闲期，不断获取id写入到缓存队列和文件
        Executors.newSingleThreadScheduledExecutor(new InternalThreadFactory("cache-push-job"))
                .scheduleWithFixedDelay(() -> {
                    try {
                        long[] ids;
                        while (true) {
                            // 每次获取前检查一下是否busy
                            boolean busy = isBusy();
                            if (busy) {
                                return;
                            }
                            long cacheSingleDiff;
                            // 单id的cache queue填充
                            if ((cacheSingleDiff = MAX_CACHE_SINGLE_QUEUE_SIZE - cacheSingleIds.size()) > 0) {
                                ids = nextIdRaw(
                                        (int) (cacheSingleDiff > MAX_SEQUENCE_COUNT ? MAX_SEQUENCE_COUNT : cacheSingleDiff));
                                // 由于有个读取文件的线程可能会在这个时候填充这个queue，这里用addAll可能会报错
//                                cacheSingleIds.addAll(LongStream.of(ids).boxed().collect(Collectors.toList()));
                                for (long id : ids) {
                                    cacheSingleIds.offer(id);
                                }
                            }
                            // 数组id的cache queue填充
                            else if (cacheArrayIds.size() < MAX_CACHE_ARRAY_QUEUE_SIZE) {
                                ids = nextIdRaw(partitionSize);
                                cacheArrayIds.offer(ids);
                            }
                            // 文件未满时,将批量获取的id写入文件
                            else if (!uidPersistence.isFull()) {
                                ids = nextIdRaw(partitionSize);
                                uidPersistence.writeToFile(ids);
                            }
                            // 如果所有缓存都存满了,则return掉结束本次任务执行
                            else {
                                return;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, 100, 100, TimeUnit.MILLISECONDS);

        Executors.newSingleThreadScheduledExecutor(new InternalThreadFactory("read-data-to-queue"))
                .scheduleWithFixedDelay(() -> {
                    try {
                        List<Long> longs = uidPersistence.readIds();
                        if (longs != null && longs.size() > 0) {
                            if (cacheArrayIds.size() < MAX_CACHE_ARRAY_QUEUE_SIZE && longs.size() >= partitionSize) {
                                long[] e = new long[partitionSize];
                                for (int i = 0; i < partitionSize; i++) {
                                    e[i] = longs.remove(0);
                                }
                                cacheArrayIds.put(e);
                            }
                            if (cacheSingleIds.size() < MAX_CACHE_SINGLE_QUEUE_SIZE) {
                                for (Long id : longs) {
                                    cacheSingleIds.put(id);
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, 100, 500, TimeUnit.MILLISECONDS);
    }

    /**
     * 初始化线程池
     */
    private ExecutorService initThreadPool() {
        // 任务执行线程
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new DistinctLinkedBlockingQueue<>(r -> {
                    // 这里只能强转
                    // 因为ThreadPoolExecutor的queue的泛型写死的是<Runnable>而不是<? extend Runnable>
                    // TODO 可以考虑自定义一个线程池
                    LoadIdTask loadIdTask = (LoadIdTask) r;
                    return loadIdTask.getCurrentIndex();
                }, 16),
                new InternalThreadFactory("load-id-to-partition"));
    }

    /**
     * 初始化填充文件内容
     */
    private void initPadFile() {
        for (int i = 0; i < INIT_FILE_SECOND; i++) {
            uidPersistence.writeToFile(nextIdRaw(MAX_SEQUENCE_COUNT));
        }
    }

    /**
     * 原子的获取下个可以使用的index
     * <p>
     * TODO 这里将是激烈竞争的地方，考虑使用分区的方式分散热点
     */
    private int nextIndex(boolean returnCurrent) {
        while (true) {
            int current = rangeIndex.get();
            int next = current >= (threshold - 1) ? 0 : current + 1;
            if (rangeIndex.compareAndSet(current, next)) {
                return returnCurrent ? current : next;
            }
        }
    }

    /**
     * 随机的下一个id
     * <p>
     * 伪随机:本质在buffer中随机出一个值来尝试获取
     */
    public long randomNextId() {

        return 0;
    }

    /**
     * 获取下一批id
     *
     * @param count 获取id的数量
     */
    public long[] nextBatchId(int count) {
        // TODO
        return null;
    }

    /**
     * 返回指定容量的2的n次方
     * 参考HashMap的实现
     */
    private static int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_SIZE) ? MAXIMUM_SIZE : n + 1;
    }

    /******************以下使用Unsafe的cas方法进行缓存区的原子操作*****************************/


    /**
     * cas替换指定位置的值
     */
    private boolean casObject(int i, IdHolder except, IdHolder update) {
        return UNSAFE.compareAndSwapObject(buffer, ((long) i << ASHIFT) + ABASE, except, update);
    }

    /**
     * 从主存中获取指定位置的元素
     */
    private IdHolder getVolatile(int i) {
        if (i < 0 || i >= threshold) {
            throw new RuntimeException("数组越界->i:" + i);
        }
        return (IdHolder) UNSAFE.getObjectVolatile(buffer, ((long) i << ASHIFT) + ABASE);
    }

    private static final Unsafe UNSAFE;
    private static final long ABASE;
    private static final int ASHIFT;

    static {
        try {
            final PrivilegedExceptionAction<Unsafe> action = () -> {
                Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                return (Unsafe) theUnsafe.get(null);
            };

            UNSAFE = AccessController.doPrivileged(action);

            // 初始化偏移量
            Class<IdHolder[]> ak = IdHolder[].class;
            ABASE = UNSAFE.arrayBaseOffset(ak);
            int scale = UNSAFE.arrayIndexScale(ak);
            if ((scale & (scale - 1)) != 0) {
                throw new Error("data type scale not a power of two");
            }
            ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load unsafe", e);
        }
    }

    public static void main(String[] args) {
        IdCacheWorker idCacheWorker = new IdCacheWorker();
        for (int i = 0; i < 10; i++) {
            System.out.println(idCacheWorker.nextId());
        }
    }
}
