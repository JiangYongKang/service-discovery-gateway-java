package com.github.highcumontoa.servicediscoverygatewayjava.web.dto;

import java.util.List;

public class BehaviorRequestDto {
    /** 固定行为；与 script 二选一 */
    private String behavior;
    /** 按调用次数依次生效的脚本行为 */
    private List<String> script;
    /** 模拟处理延迟（毫秒），用于稳定制造在途窗口以演示幂等去重 */
    private Long latencyMillis;

    public String getBehavior() { return behavior; }
    public void setBehavior(String behavior) { this.behavior = behavior; }
    public List<String> getScript() { return script; }
    public void setScript(List<String> script) { this.script = script; }
    public Long getLatencyMillis() { return latencyMillis; }
    public void setLatencyMillis(Long latencyMillis) { this.latencyMillis = latencyMillis; }
}
