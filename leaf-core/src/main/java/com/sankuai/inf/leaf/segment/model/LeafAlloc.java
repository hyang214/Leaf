package com.sankuai.inf.leaf.segment.model;

/**
 * Leaf的配置
 */
public class LeafAlloc {
    /**
     * 业务key
     */
    private String key;
    /**
     * 已经分配的最大id
     */
    private long maxId;
    /**
     * 步长
     */
    private int step;
    /**
     * 上次更新时间戳
     */
    private String updateTime;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getMaxId() {
        return maxId;
    }

    public void setMaxId(long maxId) {
        this.maxId = maxId;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
