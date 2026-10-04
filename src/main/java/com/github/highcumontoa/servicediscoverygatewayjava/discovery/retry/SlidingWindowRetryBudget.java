package com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分桶滑动窗口实现的重试预算。
 *
 * - 每服务一个窗口，窗口切成固定数量的小桶，按时间对齐滚动；
 *   桶过期即清零，窗口滑过后重试额度自然恢复，无需人工干预；
 * - 每个窗口状态的所有读写都在其 synchronized 方法内完成，
 *   并发下“判定 + 消耗”是原子的，不会超发也不会丢计；
 * - 默认配置（两个维度都关闭）下所有判定直接放行，零行为变化。
 */
public class SlidingWindowRetryBudget implements RetryBudget {

    private static final Logger log = LoggerFactory.getLogger(SlidingWindowRetryBudget.class);

    /** 窗口分桶数：桶越细窗口滑动越平滑。 */
    private static final int BUCKET_COUNT = 10;

    private final RetryBudgetConfig config;
    private final Map<String, Window> windows = new ConcurrentHashMap<>();

    public SlidingWindowRetryBudget(RetryBudgetConfig config) {
        this.config = config;
    }

    @Override
    public void recordFirstAttempt(String serviceName, long now) {
        if (config.isDisabled()) {
            return;
        }
        window(serviceName).recordFirst(now);
    }

    @Override
    public Decision tryConsumeRetry(String serviceName, long now) {
        if (config.isDisabled()) {
            return Decision.allow("retry budget disabled (default), retry allowed");
        }
        return window(serviceName).tryConsumeRetry(now, config, serviceName);
    }

    private Window window(String serviceName) {
        return windows.computeIfAbsent(serviceName,
                k -> new Window(BUCKET_COUNT, config.windowMillis()));
    }

    /** 单服务的滑动窗口状态：固定分桶 + 同步方法保证并发安全。 */
    private static final class Window {

        private static final long UNUSED = Long.MIN_VALUE;

        private final long windowMillis;
        private final long bucketMillis;
        private final long[] bucketStart;
        private final int[] firsts;
        private final int[] retries;

        Window(int bucketCount, long windowMillis) {
            this.windowMillis = windowMillis;
            this.bucketMillis = Math.max(1, windowMillis / bucketCount);
            this.bucketStart = new long[bucketCount];
            this.firsts = new int[bucketCount];
            this.retries = new int[bucketCount];
            Arrays.fill(bucketStart, UNUSED);
        }

        synchronized void recordFirst(long now) {
            roll(now);
            firsts[indexFor(now)]++;
        }

        synchronized Decision tryConsumeRetry(long now, RetryBudgetConfig config, String serviceName) {
            roll(now);
            int firstSum = 0;
            int retrySum = 0;
            long windowStart = now - windowMillis;
            for (int i = 0; i < bucketStart.length; i++) {
                if (bucketStart[i] != UNUSED && bucketStart[i] >= windowStart) {
                    firstSum += firsts[i];
                    retrySum += retries[i];
                }
            }
            String usage = "service=" + serviceName + ", window=" + windowMillis + "ms"
                    + ", retriesInWindow=" + retrySum
                    + (config.limitsTotal() ? "/" + config.maxRetriesPerWindow() : "")
                    + ", firstsInWindow=" + firstSum
                    + (config.limitsRatio() ? ", retryRatioCap=" + config.maxRetryRatio() : "");
            if (config.limitsTotal() && retrySum >= config.maxRetriesPerWindow()) {
                log.info("retry budget DENY: {} (basis: total retries in window reached cap)", usage);
                return Decision.deny("retry budget exhausted: " + usage);
            }
            if (config.limitsRatio() && retrySum + 1 > config.maxRetryRatio() * firstSum) {
                log.info("retry budget DENY: {} (basis: retry/first ratio would exceed cap)", usage);
                return Decision.deny("retry ratio budget exhausted: " + usage);
            }
            retries[indexFor(now)]++;
            log.info("retry budget ALLOW: {} (basis: within window caps)", usage);
            return Decision.allow("within retry budget: " + usage);
        }

        /** 把当前时间所在桶滚动到最新周期（过期桶清零复用）。 */
        private void roll(long now) {
            int idx = indexFor(now);
            long start = (now / bucketMillis) * bucketMillis;
            if (bucketStart[idx] != start) {
                bucketStart[idx] = start;
                firsts[idx] = 0;
                retries[idx] = 0;
            }
        }

        private int indexFor(long now) {
            return (int) Math.floorMod(now / bucketMillis, bucketStart.length);
        }
    }
}
