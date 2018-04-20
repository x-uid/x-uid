package com.zacard.core.cache;

/**
 * 填充long的类，避免伪共享
 *
 * @author guoqw
 * @since 2018-04-18 11:38
 */
public class PaddingLong {

    private long p1, p2, p3, p4, p5, p6 = 7L;
    /**
     * 阻止jvm优化掉无用的字段
     */
    public long preventOptimisation() {
        return p1 + p2 + p3 + p4 + p5 + p6;
    }
}
