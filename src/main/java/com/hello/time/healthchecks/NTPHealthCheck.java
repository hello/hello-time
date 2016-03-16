package com.hello.time.healthchecks;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import com.codahale.metrics.health.HealthCheck;

import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Set;

/**
 * Created by jnorgan on 3/16/16.
 */
public class NTPHealthCheck extends HealthCheck {

  private static final Logger LOGGER = LoggerFactory.getLogger(NTPHealthCheck.class);
  public static Integer DEFAULT_CLOCK_TOLERANCE_MILLIS = 60 * 1000;
  public static Integer DEFAULT_NTP_CLIENT_TIMEOUT_MILLIS = 2 * 1000;
  private Integer clockTolerance;
  private Integer clientTimeout;

  public NTPHealthCheck(final Integer clockTolerance, final Integer clientTimeout) {
    this.clockTolerance = clockTolerance;
    this.clientTimeout = clientTimeout;
  }

  public NTPHealthCheck(final Integer clockTolerance) {
    this(clockTolerance, DEFAULT_NTP_CLIENT_TIMEOUT_MILLIS);
  }

  public NTPHealthCheck() {
    this(DEFAULT_CLOCK_TOLERANCE_MILLIS);
  }

  @Override
  protected Result check() throws Exception {
    final Optional<Date> ntpDate = getNTPDate();
    final Date nowDate = new Date();
    if(!ntpDate.isPresent()) {
      return Result.unhealthy("Failed to get NTP time.");
    }

    if (Math.abs(nowDate.getTime() - ntpDate.get().getTime()) < clockTolerance) {
      return Result.healthy();
    } else {
      return Result.unhealthy("Server time does not match NTP.");
    }
  }

  private Optional<Date> getNTPDate() {

    final Set<String> hosts = new ImmutableSet.Builder<String>()
        .add("0.amazon.pool.ntp.org")
        .add("1.amazon.pool.ntp.org")
        .add("2.amazon.pool.ntp.org")
        .add("3.amazon.pool.ntp.org")
        .build();

    final NTPUDPClient client = new NTPUDPClient();
    client.setDefaultTimeout(this.clientTimeout);

    for (final String host : hosts) {

      try {
        final InetAddress hostAddr = InetAddress.getByName(host);
        final TimeInfo info = client.getTime(hostAddr);
        final Date date = new Date(info.getMessage().getTransmitTimeStamp().getTime());
        return Optional.of(date);
      }
      catch (IOException e) {
        LOGGER.error("error=ntp_host exception={}", e.toString());
      }
    }

    client.close();

    return null;

  }
}
