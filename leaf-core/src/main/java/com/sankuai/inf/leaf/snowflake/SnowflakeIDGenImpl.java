package com.sankuai.inf.leaf.snowflake;

import com.google.common.base.Preconditions;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class SnowflakeIDGenImpl implements IDGen {

    @Override
    public boolean init() {
        return true;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeIDGenImpl.class);
    /** 相对时间的起点 **/
    private final long twepoch;
    /** worker保留位数，2^10 = 1024 **/
    private final long workerIdBits = 10L;
    /** 最大worker编号
     * 最大能够分配的workerid =1023
     * **/
    private final long maxWorkerId = ~(-1L << workerIdBits);
    /** 毫秒内时间序列位 **/
    private final long sequenceBits = 12L;
    /** worker左移位数 **/
    private final long workerIdShift = sequenceBits;
    /** 相对时间左移位数 **/
    private final long timestampLeftShift = sequenceBits + workerIdBits;
    /** 得到前sequenceBits全部为1，其他为0的数
     * -1的补码：11111111111111111，即全为1
     * **/
    private final long sequenceMask = ~(-1L << sequenceBits);
    /** 当前发号器的worke序号，从0开始 **/
    private long workerId;
    /** 上次用的毫秒内序号 **/
    private long sequence = 0L;
    /** 上次用的毫秒时间戳 **/
    private long lastTimestamp = -1L;
    private static final Random RANDOM = new Random();

    public SnowflakeIDGenImpl(String zkAddress, int port) {
        //Thu Nov 04 2010 09:42:54 GMT+0800 (中国标准时间) 
        this(zkAddress, port, 1288834974657L);
    }

    /**
     * @param zkAddress zk地址
     * @param port      snowflake监听端口
     * @param twepoch   起始的时间戳
     */
    public SnowflakeIDGenImpl(String zkAddress, int port, long twepoch) {
        this.twepoch = twepoch;
        Preconditions.checkArgument(timeGen() > twepoch, "Snowflake not support twepoch gt currentTime");
        final String ip = Utils.getIp();
        SnowflakeZookeeperHolder holder = new SnowflakeZookeeperHolder(ip, String.valueOf(port), zkAddress);
        LOGGER.info("twepoch:{} ,ip:{} ,zkAddress:{} port:{}", twepoch, ip, zkAddress, port);
        boolean initFlag = holder.init();
        if (initFlag) {
            workerId = holder.getWorkerID();
            LOGGER.info("START SUCCESS USE ZK WORKERID-{}", workerId);
        } else {
            Preconditions.checkArgument(initFlag, "Snowflake Id Gen is not init ok");
        }
        Preconditions.checkArgument(workerId >= 0 && workerId <= maxWorkerId, "workerID must gte 0 and lte 1023");
    }

    @Override
    public synchronized Result get(String key) {
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            /** 出现时间回退，小于5毫秒 **/
            long offset = lastTimestamp - timestamp;
            if (offset <= 5) {
                try {
                    /** 等待2倍时间 **/
                    wait(offset << 1);
                    /** 重新计算 **/
                    timestamp = timeGen();
                    if (timestamp < lastTimestamp) {
                        return new Result(-1, Status.EXCEPTION);
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("wait interrupted");
                    return new Result(-2, Status.EXCEPTION);
                }
            } else {
                /** 大于5毫秒，直接返回错误 **/
                return new Result(-3, Status.EXCEPTION);
            }
        }
        /** 当前毫秒 **/
        if (lastTimestamp == timestamp) {
            /**
             * sequence + 1：序号自增
             * (sequence + 1) & sequenceMask：截断高位，防止序号超过限制
             */
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                /** 表示sequence内的值已经耗尽，需要等待下一毫秒才能发号 **/
                //seq 为0的时候表示是下一毫秒时间开始对seq做随机
                sequence = RANDOM.nextInt(100);
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            /** 如果是新的ms开始 **/
            sequence = RANDOM.nextInt(100);
        }
        lastTimestamp = timestamp;
        /**
         * timestamp - twepoch：获取相对开始时间的相对时间差
         * << timestampLeftShift：左移，将毫秒内的序列位 和 work编号位 留出来（即全是设置为0）
         * workerId << workerIdShift：work编号左移，将 将毫秒内的序列位 留出来（即全是设置为0）
         * 通过 与 得到 { 相对时间位 | work编号位 | 将毫秒内的序列位 }
         */
        long id = ((timestamp - twepoch) << timestampLeftShift) | (workerId << workerIdShift) | sequence;
        return new Result(id, Status.SUCCESS);
    }

    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }

    public long getWorkerId() {
        return workerId;
    }

}
