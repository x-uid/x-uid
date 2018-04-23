package com.zacard.core.cache;

import com.zacard.core.UidGenerator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author guoqw
 * @since 2018-04-18 15:44
 */
public class LoadIdTask implements Runnable {

    private final int currentIndex;

    private final int partitionSize;

    private final IdHolder[] buffer;

    private final BlockingQueue<long[]> queue;

    public LoadIdTask(int currentIndex,
                      int partitionSize,
                      IdHolder[] buffer,
                      BlockingQueue<long[]> queue) {
        this.currentIndex = currentIndex;
        this.partitionSize = partitionSize;
        this.buffer = buffer;
        this.queue = queue;
    }


    @Override
    public void run() {
        if (currentIndex == 0) {
            return;
        }
        // 批量生成partitionSize个唯一ID
        // 优先去二级缓存--queue里面取数据
        long[] ids = null;
//        try {
//            ids = queue.poll(10, TimeUnit.MILLISECONDS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        if (ids == null) {
            ids = UidGenerator.generateIds(partitionSize);
        }
        int firstIndex = currentIndex - partitionSize + 1;
        for (int i = 0; i < partitionSize; i++) {
            // 这里不需要原子的设置，其他读的地方会原子的读，保证获取到原子的值
            buffer[firstIndex + i] = IdHolder.fill(ids[i]);
        }
    }

    public int getCurrentIndex() {
        return currentIndex;
    }
}
