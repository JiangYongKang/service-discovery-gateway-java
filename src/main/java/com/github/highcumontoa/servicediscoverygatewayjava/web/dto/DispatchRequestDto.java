package com.github.highcumontoa.servicediscoverygatewayjava.web.dto;

public class DispatchRequestDto {
    private String payload;
    private String idempotencyKey;
    private boolean safeRetry = true;

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public boolean isSafeRetry() { return safeRetry; }
    public void setSafeRetry(boolean safeRetry) { this.safeRetry = safeRetry; }
}
