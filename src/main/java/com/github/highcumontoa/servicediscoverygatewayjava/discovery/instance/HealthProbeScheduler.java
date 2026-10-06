package com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 周期性健康探测调度器：按固定间隔对服务执行 probeOnce。
 *
 * start/stop 均在同步方法内完成，避免重复启动；单线程调度保证同一服务的探测轮次
 * 不会自身重叠（注册中心内部还对单实例探测加了细粒度锁）。
 */
public class HealthProbeScheduler {

    private static final Logger log = LoggerFactory.getLogger(HealthProbeScheduler.class);

    private final ServiceRegistry registry;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> task;
    private String serviceName;
    /**
     * 每轮健康探测后附带执行的附加扫描（如熔断器的多实例恢复探测）。
     * 它只是被调度触发，真正的恢复探测在转发器自己的线程池里按实例并行执行，
     * 因此多实例同时到期时互不阻塞，也不会卡住健康探测调度线程。
     */
    private Runnable extraRound;

    public HealthProbeScheduler(ServiceRegistry registry) {
        this.registry = registry;
    }

    public synchronized void start(String serviceName, long intervalMillis) {
        start(serviceName, intervalMillis, null);
    }

    public synchronized void start(String serviceName, long intervalMillis, Runnable extraRound) {
        if (task != null) {
            log.info("probe scheduler already running for {}, skip start", this.serviceName);
            return;
        }
        this.serviceName = serviceName;
        this.extraRound = extraRound;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "health-probe-" + serviceName);
            t.setDaemon(true);
            return t;
        });
        this.task = executor.scheduleWithFixedDelay(() -> {
            try {
                registry.probeOnce(serviceName);
            } catch (Throwable t) {
                // 调度线程绝不能因单轮异常而静默终止
                log.error("probe round failed for service={}", serviceName, t);
            }
            try {
                if (this.extraRound != null) {
                    this.extraRound.run();
                }
            } catch (Throwable t) {
                log.error("extra recovery round failed for service={}", serviceName, t);
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("probe scheduler started: service={}, intervalMillis={}, extraRecoveryRound={}",
                serviceName, intervalMillis, extraRound != null);
    }

    public synchronized void stop() {
        if (task != null) {
            task.cancel(false);
            task = null;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        log.info("probe scheduler stopped for {}", serviceName);
    }
}
