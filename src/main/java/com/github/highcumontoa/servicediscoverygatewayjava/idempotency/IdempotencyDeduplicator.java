package com.github.highcumontoa.servicediscoverygatewayjava.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 幂等去重：相同 idempotencyKey 的并发请求共享同一次执行。
 * - 首个请求 {@code tryAcquire} 返回 Optional.empty()，由调用方真正执行；
 * - 并发重复请求返回在途 future 以复用结果，绝不触发第二次下游执行；
 * - 执行结束（成功或失败）后 {@code release} 移除条目，后续同 key 请求作为新请求执行。
 */
public class IdempotencyDeduplicator {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyDeduplicator.class);

    private final ConcurrentMap<String, CompletableFuture<Object>> inflight = new ConcurrentHashMap<>();

    /** 尝试占位：已存在在途执行则返回它，否则放入占位 future 并返回 empty。 */
    public Optional<CompletableFuture<Object>> tryAcquire(String key, CompletableFuture<Object> execution) {
        CompletableFuture<Object> existing = inflight.putIfAbsent(key, execution);
        if (existing != null) {
            log.info("[idempotency] key={} 命中在途执行，复用结果，不重复调用下游", key);
            return Optional.of(existing);
        }
        log.info("[idempotency] key={} 占位成功，由当前请求执行一次", key);
        return Optional.empty();
    }

    /** 执行结束后清理占位：仅当占位仍是本次 future 时移除，避免误删下一轮执行。 */
    public void release(String key, CompletableFuture<Object> execution) {
        boolean removed = inflight.remove(key, execution);
        if (removed) {
            log.info("[idempotency] key={} 执行结束，释放占位", key);
        }
    }
}
