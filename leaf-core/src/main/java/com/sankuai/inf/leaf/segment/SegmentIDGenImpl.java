package com.sankuai.inf.leaf.segment;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.model.*;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentIDGenImpl implements IDGen {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIDGenImpl.class);

    /**
     * IDCache未初始化成功时的异常码
     */
    private static final long EXCEPTION_ID_IDCACHE_INIT_FALSE = -1;
    /**
     * key不存在时的异常码
     */
    private static final long EXCEPTION_ID_KEY_NOT_EXISTS = -2;
    /**
     * SegmentBuffer中的两个Segment均未从DB中装载时的异常码
     */
    private static final long EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL = -3;
    /**
     * 最大步长不超过100,0000
     */
    private static final int MAX_STEP = 1000000;
    /**
     * 一个Segment维持时间为15分钟
     */
    private static final long SEGMENT_DURATION = 15 * 60 * 1000L;
    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new UpdateThreadFactory());
    private volatile boolean initOK = false;
    /**
     * 内存中缓存的，发号器配置数据，通过定时任务，与数据库中的新增和删除比较
     * 但是发号的进度信息，不从数据库拉取，而是内存中维护
     */
    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<String, SegmentBuffer>();
    private IDAllocDao dao;

    /**
     * 后备号段更新线程工厂
     */
    public static class UpdateThreadFactory implements ThreadFactory {

        private static int threadInitNumber = 0;

        private static synchronized int nextThreadNum() {
            return threadInitNumber++;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Thread-Segment-Update-" + nextThreadNum());
        }
    }

    @Override
    public boolean init() {
        logger.info("Init ...");
        /** 确保加载到kv后才初始化成功
         * 从数据库拉取配置到内存，提高性能
         * **/
        updateCacheFromDb();
        initOK = true;
        /**
         * 启动一个线程，异步去从数据库拉取最新的数据
         */
        updateCacheFromDbAtEveryMinute();
        return initOK;
    }

    /**
     * 启动一个定时线程
     * 每一分钟，进行一次数据更新
     */
    private void updateCacheFromDbAtEveryMinute() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("check-idCache-thread");
                t.setDaemon(true);
                return t;
            }
        });
        service.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                updateCacheFromDb();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    /**
     * 从数据库拉取配置，本方法用于首次初始化，也用于后序定时更新，
     * 所以逻辑是当内存中已经有数据的情况进行编写的
     */
    private void updateCacheFromDb() {
        logger.info("update cache from db");
        StopWatch sw = new Slf4JStopWatch();
        try {
            /** 拉取全部业务标识，如果没有，结束 **/
            List<String> dbTags = dao.getAllTags();
            if (dbTags == null || dbTags.isEmpty()) {
                return;
            }
            /** 对比数据库中的数据 和 内存中的数据 对比
             * 或者 新增 和 删除 的业务
             * **/
            List<String> cacheTags = new ArrayList<String>(cache.keySet());
            Set<String> insertTagsSet = new HashSet<>(dbTags);
            Set<String> removeTagsSet = new HashSet<>(cacheTags);
            //db中新加的tags灌进cache
            for(int i = 0; i < cacheTags.size(); i++){
                String tmp = cacheTags.get(i);
                if(insertTagsSet.contains(tmp)){
                    insertTagsSet.remove(tmp);
                }
            }
            /** 新增业务，放入数据库中 **/
            for (String tag : insertTagsSet) {
                /** 新建的对象，初始化状态为false **/
                SegmentBuffer buffer = new SegmentBuffer();
                buffer.setKey(tag);
                Segment segment = buffer.getCurrent();
                segment.setValue(new AtomicLong(0));
                /**
                 * 为什么max 和 step都是 0
                 * {@link SegmentIDGenImpl#get(String)}查询的时候回触发如下方法
                 * {@link SegmentIDGenImpl#updateSegmentFromDb}，中会拉取数据库的配置，将max 和 step写入
                 */
                segment.setMax(0);
                segment.setStep(0);
                cache.put(tag, buffer);
                logger.info("Add tag {} from db to IdCache, SegmentBuffer {}", tag, buffer);
            }
            /** 筛选出已经删除的标签 **/
            for(int i = 0; i < dbTags.size(); i++){
                String tmp = dbTags.get(i);
                if(removeTagsSet.contains(tmp)){
                    removeTagsSet.remove(tmp);
                }
            }
            /** cache中已失效的tags从cache删除 **/
            for (String tag : removeTagsSet) {
                cache.remove(tag);
                logger.info("Remove tag {} from IdCache", tag);
            }
        } catch (Exception e) {
            logger.warn("update cache from db exception", e);
        } finally {
            sw.stop("updateCacheFromDb");
        }
    }

    /**
     * 根据业务key进行发号
     * @param key
     * @return
     */
    @Override
    public Result get(final String key) {
        /**
         * 是否初始化化完成
         */
        if (!initOK) {
            return new Result(EXCEPTION_ID_IDCACHE_INIT_FALSE, Status.EXCEPTION);
        }
        /** 如果不是，已有的key，那么异常抛出 **/
        if (cache.containsKey(key)) {
            /** 从缓存中获取分段信息 **/
            SegmentBuffer buffer = cache.get(key);
            /** 如果分段未初始化完成，进行初始化 **/
            if (!buffer.isInitOk()) {
                /** 对buffer进行互斥，只能一个请求进入 **/
                synchronized (buffer) {
                    /** 如果分段未初始化完成，进行初始化
                     * double check。防止多次初始化
                     * 如果不进行检验，会在如下情况下，出现重复初始化
                     * Thread A：  判断okay(不okay)                                       加锁   初始化  释放
                     * Thread B：                  判断okay(不okay)   加锁   初始化  释放
                     * 如果进行校验，那么就会按照如下流程，不出现重复初始化
                     * Thread A：  判断okay(不okay)                                                        判断okay(okay)  跳过
                     * Thread B：                  判断okay(不okay):   加锁   判断okay(不okay) 初始化  释放
                     * **/
                    if (!buffer.isInitOk()) {
                        try {
                            /**
                             * buffer.getCurrent(): 获取当前工作分段
                             * updateSegmentFromDb(key, buffer.getCurrent())
                             */
                            updateSegmentFromDb(key, buffer.getCurrent());
                            logger.info("Init buffer. Update leafkey {} {} from db", key, buffer.getCurrent());
                            buffer.setInitOk(true);
                        } catch (Exception e) {
                            logger.warn("Init buffer {} exception", buffer.getCurrent(), e);
                        }
                    }
                }
            }
            /** 进行发号操作 **/
            return getIdFromSegmentBuffer(cache.get(key));
        }
        return new Result(EXCEPTION_ID_KEY_NOT_EXISTS, Status.EXCEPTION);
    }

    /**
     * 从数据库拉取数据初始化分段
     * @param key
     * @param segment
     */
    public void updateSegmentFromDb(String key, Segment segment) {
        /** 用于性能监控 **/
        StopWatch sw = new Slf4JStopWatch();
        SegmentBuffer buffer = segment.getBuffer();
        LeafAlloc leafAlloc;
        if (!buffer.isInitOk()) {
            /** 用于初始化的分支 **/
            /** 如果没有初始化，从数据库拉取数据 **/
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            /** 更新 max 和 step
             * 如果未初始化，这2个值都是0
             * **/
            buffer.setStep(leafAlloc.getStep());
            /** leafAlloc中的step为DB中的step **/
            buffer.setMinStep(leafAlloc.getStep());
        } else if (buffer.getUpdateTimestamp() == 0) {
            /** 第一次 **/
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(leafAlloc.getStep());
            /** leafAlloc中的step为DB中的step **/
            buffer.setMinStep(leafAlloc.getStep());
        } else {
            long duration = System.currentTimeMillis() - buffer.getUpdateTimestamp();
            int nextStep = buffer.getStep();
            if (duration < SEGMENT_DURATION) {
                if (nextStep * 2 > MAX_STEP) {
                    //do nothing
                } else {
                    nextStep = nextStep * 2;
                }
            } else if (duration < SEGMENT_DURATION * 2) {
                //do nothing with nextStep
            } else {
                nextStep = nextStep / 2 >= buffer.getMinStep() ? nextStep / 2 : nextStep;
            }
            logger.info("leafKey[{}], step[{}], duration[{}mins], nextStep[{}]", key, buffer.getStep(), String.format("%.2f",((double)duration / (1000 * 60))), nextStep);
            LeafAlloc temp = new LeafAlloc();
            temp.setKey(key);
            temp.setStep(nextStep);
            leafAlloc = dao.updateMaxIdByCustomStepAndGetLeafAlloc(temp);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(nextStep);
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc的step为DB中的step
        }
        /** must set value before set max
         * value 设置为eafAlloc.getMaxId() - buffer.getStep()，即更新前的最大号码
         * max 设置为更新后的最大值
         * step 步长为当前buffer的步长
         * **/
        long value = leafAlloc.getMaxId() - buffer.getStep();
        segment.getValue().set(value);
        segment.setMax(leafAlloc.getMaxId());
        segment.setStep(buffer.getStep());
        sw.stop("updateSegmentFromDb", key + " " + segment);
    }

    /**
     * 进行发号
     * @param buffer
     * @return
     */
    public Result getIdFromSegmentBuffer(final SegmentBuffer buffer) {
        while (true) {
            /** 对号段进行加读锁 **/
            buffer.rLock().lock();
            try {
                final Segment segment = buffer.getCurrent();
                /**
                 * 对后备号段进行更新
                 * 条件如下：
                 *      + 后备号段未ready
                 *      + 本号段剩余可用号码，少于90%步长
                 *      + 当前没有号段更新线程在执行
                 *          + 然后通过cas操作，将状态改成执行
                 * **/
                if (!buffer.isNextReady() && (segment.getIdle() < 0.9 * segment.getStep()) && buffer.getThreadRunning().compareAndSet(false, true)) {
                    /** 通过线程池，限制更新的线程数目
                     * 因为同时可能有很多业务公用发号器
                     * **/
                    service.execute(new Runnable() {
                        @Override
                        public void run() {
                            /** 获取下一次的segment **/
                            Segment next = buffer.getSegments()[buffer.nextPos()];
                            /** 记录是否更新成功 **/
                            boolean updateOk = false;
                            try {
                                updateSegmentFromDb(buffer.getKey(), next);
                                updateOk = true;
                                logger.info("update segment {} from db {}", buffer.getKey(), next);
                            } catch (Exception e) {
                                logger.warn(buffer.getKey() + " updateSegmentFromDb exception", e);
                            } finally {
                                /** 更新成功 **/
                                if (updateOk) {
                                    /**
                                     * 加写锁
                                     * 修改后备号段的状态
                                     * 修改线程状态
                                     * 解写锁
                                     * **/
                                    buffer.wLock().lock();
                                    buffer.setNextReady(true);
                                    buffer.getThreadRunning().set(false);
                                    buffer.wLock().unlock();
                                } else {
                                    /** 更新失败
                                     * 将线程执行状态改成false
                                     * 因为申请需要通过cas，所以在这种情况下，
                                     * 不需要进行加锁，其他线程不可能将 threadRunning改成true
                                     * **/
                                    buffer.getThreadRunning().set(false);
                                }
                            }
                        }
                    });
                }
                /**
                 * 号段进行发号
                 * + value自增
                 * 如果没到最大的值(不包括)，那么说明okay
                 */
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
            } finally {
                /** 对号段释放读锁 **/
                buffer.rLock().unlock();
            }
            /** 当前号段耗尽的情况 **/
            /** 进入等待 **/
            waitAndSleep(buffer);
            buffer.wLock().lock();
            try {
                /**
                 * 为什么还是用当前的segment，这个segment，不应该已经耗尽了么
                 * + 等待期间，可能由其他的请求线程，完成了号段的切换
                 * + 再次尝试，用"当前"号段进行切换
                 */
                final Segment segment = buffer.getCurrent();
                /** 进行发号，成功就返回 **/
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
                /** 未完成号段切换，进行切换
                 * + 将currentPos 自增1
                 * + 将后备号段状态改成false，即不可用
                 * **/
                if (buffer.isNextReady()) {
                    buffer.switchPos();
                    buffer.setNextReady(false);
                } else {
                    /** 未切换完成，异常 **/
                    logger.error("Both two segments in {} are not ready!", buffer);
                    return new Result(EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL, Status.EXCEPTION);
                }
            } finally {
                /** 进行解锁 **/
                buffer.wLock().unlock();
            }
        }
    }

    /**
     * 循环等待后备线程更新执行完成
     * + 参数等待，乐观状态，如果实在没等待到，依旧结束，由外部的调用方进行异常处理
     * @param buffer
     */
    private void waitAndSleep(SegmentBuffer buffer) {
        int roll = 0;
        /** 如果更新线程还在执行，就持续等待 **/
        while (buffer.getThreadRunning().get()) {
            roll += 1;
            if(roll > 10000) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                    break;
                } catch (InterruptedException e) {
                    logger.warn("Thread {} Interrupted",Thread.currentThread().getName());
                    break;
                }
            }
        }
    }

    /**
     * 查询全部配置
     * @return
     */
    public List<LeafAlloc> getAllLeafAllocs() {
        return dao.getAllLeafAllocs();
    }

    public Map<String, SegmentBuffer> getCache() {
        return cache;
    }

    public IDAllocDao getDao() {
        return dao;
    }

    public void setDao(IDAllocDao dao) {
        this.dao = dao;
    }
}
