package com.jay.swarm.common.util;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *  调度器组件
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 10:49
 */
public class ScheduleUtil {
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        private AtomicInteger threadId = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "schedule-thread-" + threadId.getAndIncrement());
        }
    });
}
