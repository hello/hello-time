package com.hello.time.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hello.suripu.coredropwizard.configuration.GraphiteConfiguration;
import com.hello.suripu.coredropwizard.configuration.NewDynamoDBConfiguration;

import io.dropwizard.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class SuripuConfiguration extends Configuration {

    @Valid
    @NotNull
    @JsonProperty("metrics_enabled")
    private Boolean metricsEnabled;

    public Boolean getMetricsEnabled() {
        return metricsEnabled;
    }

    @Valid
    @JsonProperty("debug")
    private Boolean debug = Boolean.FALSE;
    public Boolean getDebug() {
        return debug;
    }

    @Valid
    @JsonProperty("ntp_clock_tolerance")
    private Integer ntpClockTolerance;
    public Integer getNtpClockTolerance() {
        return ntpClockTolerance;
    }

    @Valid
    @JsonProperty("ntp_client_timeout")
    private Integer ntpClientTimeout;
    public Integer getNtpClientTimeout() {
        return ntpClientTimeout;
    }

    @Valid
    @NotNull
    @JsonProperty("graphite")
    private GraphiteConfiguration graphite;

    public GraphiteConfiguration getGraphite() {
        return graphite;
    }

    @Valid
    @NotNull
    @JsonProperty("dynamodb")
    private NewDynamoDBConfiguration dynamoDBConfiguration;

    public NewDynamoDBConfiguration dynamoDBConfiguration(){
        return dynamoDBConfiguration;
    }
}
