package com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry;

import java.util.function.Supplier;

/**
 * 幂等去重：同一幂等键的并发请求只执行一次，其余等待并共享同一结果。
 *
 * @param <T> 执行结果类型
 */
public interface IdempotencyDeduplicator<T> {

    /**
     * 按 key 去重执行。
     *
     * @param key      幂等键（null/空串表示不去重）
     * @param supplier 实际执行逻辑
     * @return 执行结果（后到并发者拿到首个请求的结果）
     */
    T execute(String key, Supplier<T> supplier);

    /** 当前已缓存的键数量（测试/观测用）。 */
    int cachedKeys();
}
