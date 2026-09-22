package com.github.highcumontoa.servicediscoverygatewayjava.web;

import com.github.highcumontoa.servicediscoverygatewayjava.gateway.ForwardingEngine;
import com.github.highcumontoa.servicediscoverygatewayjava.model.AttemptFailure;
import com.github.highcumontoa.servicediscoverygatewayjava.model.DispatchRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.model.DispatchResult;
import com.github.highcumontoa.servicediscoverygatewayjava.web.dto.DispatchRequestDto;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 网关转发接口。
 */
@RestController
@RequestMapping("/gateway")
public class GatewayController {

    private final ForwardingEngine engine;

    public GatewayController(ForwardingEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/services/{serviceName}/invoke")
    public Map<String, Object> invoke(@PathVariable String serviceName,
                                      @RequestBody(required = false) DispatchRequestDto dto) {
        DispatchRequestDto body = dto == null ? new DispatchRequestDto() : dto;
        DispatchRequest request = new DispatchRequest(serviceName, body.getPayload(),
                body.getIdempotencyKey(), body.isSafeRetry());
        DispatchResult result = engine.forward(request);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("instanceId", result.instanceId());
        response.put("statusCode", result.statusCode());
        response.put("body", result.body());
        response.put("attempts", result.attempts());
        response.put("duplicated", result.duplicated());
        return response;
    }
}
