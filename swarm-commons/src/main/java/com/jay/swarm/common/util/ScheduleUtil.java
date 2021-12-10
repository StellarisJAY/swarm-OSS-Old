package com.jay.swarm.common.util;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *  调度器组件
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 10:49
 */
@Slf4j
public class ScheduleUtil {
    private final static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        private AtomicInteger threadId = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "schedule-thread-" + threadId.getAndIncrement());
        }
    });

    public static void scheduleAtFixedRate(Runnable runnable, long delay, long period, TimeUnit timeUnit){
        if(delay < 0 || period <= 0 || timeUnit == null){
            throw new IllegalArgumentException("illegal argument for scheduler");
        }
        if(runnable != null){
            Runnable task = buildTask(runnable);
            executor.scheduleAtFixedRate(task, delay, period, timeUnit);
        }
        else{
            throw new NullPointerException("missing runnable for scheduler");
        }
    }

    public static void schedule(Runnable runnable, long delay, TimeUnit timeUnit){
        if(runnable == null || delay < 0 || timeUnit == null){
            throw new IllegalArgumentException("Illegal argument for scheduler");
        }
        else{
            executor.schedule(runnable, delay, timeUnit);
        }
    }

    private static Runnable buildTask(Runnable runnable){
        return ()->{
            try{
                runnable.run();
            }catch (Exception e){
                log.error("schedule task error: ", e);
            }
        };
    }
}
