package com.github.highcumontoa.servicediscoverygatewayjava.web;

import com.github.highcumontoa.servicediscoverygatewayjava.model.GatewayException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 把网关失败映射为携带可区分 failureType 的 HTTP 响应，便于本地验证失败原因。
 */
@RestControllerAdvice
public class GatewayExceptionHandler {

    @ExceptionHandler(GatewayException.class)
    public ResponseEntity<Map<String, Object>> handle(GatewayException e) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("success", false);
        body.put("failureType", e.getFailureType());
        body.put("message", e.getMessage());
        HttpStatus httpStatus = switch (e.getFailureType()) {
            case NO_HEALTHY_INSTANCE, CIRCUIT_OPEN -> HttpStatus.SERVICE_UNAVAILABLE;
            case NON_RETRYABLE_REQUEST -> HttpStatus.BAD_GATEWAY;
            default -> HttpStatus.BAD_GATEWAY;
        };
        return ResponseEntity.status(httpStatus).body(body);
    }
}
