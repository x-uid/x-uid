package com.zacard.core.queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 会根据策略去重的阻塞队列
 *
 * @author guoqw
 * @since 2018-04-19 10:37
 */
public class DistinctLinkedBlockingQueue<E, R> extends LinkedBlockingQueue<E> {

    private static final long serialVersionUID = -1170786830150679807L;

    private final Function<E, R> keyMapper;

    private final int size;

    private final ConcurrentHashMap<R, E> distinct;

    public DistinctLinkedBlockingQueue(Function<E, R> keyMapper, int size) {
        this.keyMapper = keyMapper;
        this.size = size;
        this.distinct = new ConcurrentHashMap<>(size);
    }


    @Override
    public void put(E e) throws InterruptedException {
        E oldValue = distinct.putIfAbsent(key(e), e);
        if (oldValue != null) {
            return;
        }
        super.put(e);
    }

    @Override
    public E take() throws InterruptedException {
        E e = super.take();
        if (e != null) {
            distinct.remove(key(e));
        }
        return e;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E e = super.poll(timeout, unit);
        beforeRemove(e);
        return e;
    }

    @Override
    public E poll() {
        E e = super.poll();
        beforeRemove(e);
        return e;
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        boolean offer = super.offer(e, timeout, unit);
        if (offer) {
            distinct.putIfAbsent(key(e), e);
        }
        return offer;
    }

    @Override
    public boolean offer(E e) {
        boolean offer = super.offer(e);
        if (offer) {
            distinct.putIfAbsent(key(e), e);
        }
        return offer;
    }

    /**
     * 获取去重的比较key
     */
    private R key(E e) {
        return keyMapper.apply(e);
    }

    private void beforeRemove(E e) {
        if (e != null) {
            distinct.remove(key(e));
        }
    }
}
