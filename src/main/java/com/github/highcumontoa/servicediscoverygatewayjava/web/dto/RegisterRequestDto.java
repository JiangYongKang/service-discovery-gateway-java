package com.github.highcumontoa.servicediscoverygatewayjava.web.dto;

public class RegisterRequestDto {
    private String serviceName;
    private String instanceId;
    private String host;
    private Integer port;
    /** NORMAL / TIMEOUT / REFUSE / ERROR，缺省 NORMAL */
    private String behavior;

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }
    public String getInstanceId() { return instanceId; }
    public void setInstanceId(String instanceId) { this.instanceId = instanceId; }
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public Integer getPort() { return port; }
    public void setPort(Integer port) { this.port = port; }
    public String getBehavior() { return behavior; }
    public void setBehavior(String behavior) { this.behavior = behavior; }
}
