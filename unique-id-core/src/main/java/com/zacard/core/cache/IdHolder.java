package com.zacard.core.cache;

/**
 * 唯一id包装类
 *
 * @author guoqw
 * @since 2018-04-18 11:35
 */
public class IdHolder extends PaddingLong {

    private long value;

    public static IdHolder fill(long value) {
        IdHolder idHolder = new IdHolder();
        idHolder.value = value;
        return idHolder;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
