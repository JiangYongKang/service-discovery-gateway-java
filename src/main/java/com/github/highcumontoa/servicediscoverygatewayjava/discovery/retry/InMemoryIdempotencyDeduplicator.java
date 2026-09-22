package com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 进程内幂等去重器（结果类型泛型化，可复用于任意执行链）。
 *
 * - ConcurrentHashMap#computeIfAbsent 保证同一 key 只有一个线程进入执行体；
 * - 后到请求等待同一个 CompletableFuture，成功结果/运行时异常共享；
 * - 成功条目保留缓存供后续复用；失败条目在映射函数返回后移除（避免毒缓存，
 *   且不在映射函数内回写同一 map，否则会触发 ConcurrentHashMap 递归更新）。
 */
public class InMemoryIdempotencyDeduplicator<T> implements IdempotencyDeduplicator<T> {

    private static final Logger log = LoggerFactory.getLogger(InMemoryIdempotencyDeduplicator.class);

    private final Map<String, CompletableFuture<T>> inflight = new ConcurrentHashMap<>();

    @Override
    public T execute(String key, Supplier<T> supplier) {
        if (key == null || key.isEmpty()) {
            return supplier.get();
        }
        boolean[] failed = {false};
        CompletableFuture<T> future = inflight.computeIfAbsent(key, k -> {
            log.info("idempotency key={}: leader request starts execution", k);
            CompletableFuture<T> f = new CompletableFuture<>();
            try {
                f.complete(supplier.get());
            } catch (Throwable t) {
                failed[0] = true;
                f.completeExceptionally(t);
            }
            return f;
        });
        if (failed[0]) {
            inflight.remove(key, future);
        }
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw e;
        }
    }

    @Override
    public int cachedKeys() {
        return inflight.size();
    }

    /** 测试辅助：清理缓存。 */
    public void clear() {
        inflight.clear();
    }
}
