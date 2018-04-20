package com.zacard.core.cache;

import com.zacard.core.UidGenerator;

/**
 * @author guoqw
 * @since 2018-04-18 15:44
 */
public class LoadIdTask implements Runnable {

    private final int currentIndex;

    private final int partitionSize;

    private final IdHolder[] buffer;

    public LoadIdTask(int currentIndex,
                      int partitionSize,
                      IdHolder[] buffer) {
        this.currentIndex = currentIndex;
        this.partitionSize = partitionSize;
        this.buffer = buffer;
    }


    @Override
    public void run() {
        if (currentIndex == 0) {
            return;
        }
        for (int i = 0; i < partitionSize; i++) {
            int index = currentIndex - i;
            // 这里不需要原子的设置，其他读的地方会原子的读，保证获取到原子的值
            buffer[index] = IdHolder.fill(UidGenerator.generateId());
        }
    }

    public int getCurrentIndex() {
        return currentIndex;
    }
}
