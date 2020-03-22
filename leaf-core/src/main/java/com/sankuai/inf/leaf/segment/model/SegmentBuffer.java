package com.sankuai.inf.leaf.segment.model;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 双buffer
 */
public class SegmentBuffer {
    /**
     *  key: 业务标识，不同业务的发号是隔离的
     */
    private String key;
    /**
     * 双buffer分段，2个buffer依次用，防止一个buffer耗尽的情况
     */
    private Segment[] segments;
    /**
     * 当前的使用的segment的index
     * 通过 currentPox的累加，然后对2取余，来实现2个segment的切换
     */
    private volatile int currentPos;
    /**
     * 下一个segment是否处于可切换状态
     */
    private volatile boolean nextReady;
    /**
     * 是否初始化完成
     */
    private volatile boolean initOk;
    /**
     * 号段拉取线程是否在运行中
     */
    private final AtomicBoolean threadRunning;
    /**
     * buffer的读写锁
     */
    private final ReadWriteLock lock;

    /**
     * 步长
     */
    private volatile int step;
    /**
     * 最小步长
     */
    private volatile int minStep;
    /**
     * 更新时间
     */
    private volatile long updateTimestamp;

    public SegmentBuffer() {
        /** 创建2个buffer **/
        segments = new Segment[]{new Segment(this), new Segment(this)};
        /** pos是0，还未开始发号 **/
        currentPos = 0;
        /** 下一个segment是否处于未就绪 **/
        nextReady = false;
        /** 未初始化完成 **/
        initOk = false;
        /** 线程未运行 **/
        threadRunning = new AtomicBoolean(false);
        /** 可重入读写锁 **/
        lock = new ReentrantReadWriteLock();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Segment[] getSegments() {
        return segments;
    }

    public Segment getCurrent() {
        return segments[currentPos];
    }

    public int getCurrentPos() {
        return currentPos;
    }

    public int nextPos() {
        return (currentPos + 1) % 2;
    }

    public void switchPos() {
        currentPos = nextPos();
    }

    public boolean isInitOk() {
        return initOk;
    }

    public void setInitOk(boolean initOk) {
        this.initOk = initOk;
    }

    public boolean isNextReady() {
        return nextReady;
    }

    public void setNextReady(boolean nextReady) {
        this.nextReady = nextReady;
    }

    public AtomicBoolean getThreadRunning() {
        return threadRunning;
    }

    public Lock rLock() {
        return lock.readLock();
    }

    public Lock wLock() {
        return lock.writeLock();
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public int getMinStep() {
        return minStep;
    }

    public void setMinStep(int minStep) {
        this.minStep = minStep;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SegmentBuffer{");
        sb.append("key='").append(key).append('\'');
        sb.append(", segments=").append(Arrays.toString(segments));
        sb.append(", currentPos=").append(currentPos);
        sb.append(", nextReady=").append(nextReady);
        sb.append(", initOk=").append(initOk);
        sb.append(", threadRunning=").append(threadRunning);
        sb.append(", step=").append(step);
        sb.append(", minStep=").append(minStep);
        sb.append(", updateTimestamp=").append(updateTimestamp);
        sb.append('}');
        return sb.toString();
    }
}
