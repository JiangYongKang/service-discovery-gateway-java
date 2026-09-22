package com.github.highcumontoa.servicediscoverygatewayjava.model;

/**
 * 一次健康探测的结果。
 */
public record ProbeResult(boolean healthy, String reason) {

    public static ProbeResult up(String reason) {
        return new ProbeResult(true, reason);
    }

    public static ProbeResult down(String reason) {
        return new ProbeResult(false, reason);
    }
}
