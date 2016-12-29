package com.hello.time.resources;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.protobuf.TextFormat;
import com.hello.dropwizard.mikkusu.helpers.AdditionalMediaTypes;
import com.hello.suripu.api.ntp.Ntp;
import com.hello.suripu.core.db.KeyStore;
import com.hello.suripu.core.db.KeyStoreDynamoDB;
import com.hello.suripu.core.flipper.FeatureFlipper;
import com.hello.suripu.core.flipper.GroupFlipper;
import com.hello.suripu.core.util.HelloHttpHeader;
import com.hello.suripu.core.util.SignedMessage;
import com.hello.suripu.coredropwizard.resources.BaseResource;
import com.librato.rollout.RolloutClient;
import io.dropwizard.jersey.caching.CacheControl;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.net.ntp.TimeStamp;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import static com.codahale.metrics.MetricRegistry.name;


@Path("/")
public class TimeResource extends BaseResource {

    @Inject
    RolloutClient featureFlipper;

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeResource.class);
    private static final String LOCAL_OFFICE_IP_ADDRESS = "204.28.123.251";
    private static final String FIRMWARE_DEFAULT = "0";
    private static final int CLOCK_SKEW_TOLERATED_IN_HOURS = 2;
    private static final int CLOCK_DRIFT_MEASUREMENT_THRESHOLD = 2;

    private final KeyStore keyStore;
    private final GroupFlipper groupFlipper;
    private final MetricRegistry metrics;
    protected Meter senseClockOutOfSync;
    protected Histogram drift;

    @Context
    HttpServletRequest request;

    public TimeResource(final KeyStore keyStore,
                        final GroupFlipper groupFlipper,
                        final MetricRegistry metricRegistry) {

        this.keyStore = keyStore;
        this.metrics= metricRegistry;
        this.groupFlipper = groupFlipper;
        this.senseClockOutOfSync = metrics.meter(name(TimeResource.class, "sense-clock-out-sync"));
        this.drift = metrics.histogram(name(TimeResource.class, "sense-drift"));
    }

    @CacheControl(noCache = true)
    @POST
    @Path("/")
    @Consumes(AdditionalMediaTypes.APPLICATION_PROTOBUF)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed
    public byte[] receiveTimeRequest(final byte[] body) {

        final TimeStamp ntpReceiveTimestamp = new TimeStamp(new Date());
        final SignedMessage signedMessage = SignedMessage.parse(body);

        String debugSenseId = this.request.getHeader(HelloHttpHeader.SENSE_ID);
        if (debugSenseId == null) {
            debugSenseId = "";
        }
        final String ipAddress = getIpAddress(request);
        final String topFW = (this.request.getHeader(HelloHttpHeader.TOP_FW_VERSION) != null) ? this.request.getHeader(HelloHttpHeader.TOP_FW_VERSION) : FIRMWARE_DEFAULT;
        final String middleFW = (this.request.getHeader(HelloHttpHeader.MIDDLE_FW_VERSION) != null) ? this.request.getHeader(HelloHttpHeader.MIDDLE_FW_VERSION) : FIRMWARE_DEFAULT;

        LOGGER.info("action=request-time device_id={} ip_address={} top_fw={} middle_fw={}", debugSenseId, ipAddress, topFW, middleFW);

        Ntp.NTPDataPacket data = null;

        try {
            data = Ntp.NTPDataPacket.parseFrom(signedMessage.body);
        } catch (IOException exception) {
            final String errorMessage = String.format("Failed parsing protobuf for deviceId = %s : %s", debugSenseId, exception.getMessage());
            LOGGER.error(errorMessage);
            return plainTextError(Response.Status.BAD_REQUEST, "bad request");
        }
        LOGGER.debug("Received protobuf message {}", TextFormat.shortDebugString(data));

        if (!data.hasOriginTs()) {
            LOGGER.error("error=empty-origin_ts device_id={}", debugSenseId);
            return plainTextError(Response.Status.BAD_REQUEST, "empty origin timestamp");
        }

        final String deviceId = debugSenseId;
        final List<String> groups = groupFlipper.getGroups(deviceId);
        final List<String> ipGroups = groupFlipper.getGroups(ipAddress);


        if (featureFlipper.deviceFeatureActive(FeatureFlipper.PRINT_RAW_PB, deviceId, groups)) {
            LOGGER.info("RAW_PB for device_id={} {}", deviceId, Hex.encodeHexString(body));
        }


        final Optional<byte[]> optionalKeyBytes = getKey(deviceId, groups, ipAddress);

        if (!optionalKeyBytes.isPresent()) {
            LOGGER.error("error=key-store-failure sense_id={}", deviceId);
            return plainTextError(Response.Status.BAD_REQUEST, "");
        }

        final Optional<SignedMessage.Error> error = signedMessage.validateWithKey(optionalKeyBytes.get());

        if (error.isPresent()) {
            LOGGER.error("error=invalid_key device_id={} key={}...", deviceId, Hex.encodeHexString(optionalKeyBytes.get()).substring(0, 8));
            return plainTextError(Response.Status.UNAUTHORIZED, "");
        }
        LOGGER.debug("{}", ntpReceiveTimestamp.toDateString());

        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        final TimeStamp ntpOriginTimestamp = new TimeStamp(data.getOriginTs());
        if(isClockOutOfSync(new DateTime(ntpOriginTimestamp.getDate()), new DateTime(ntpReceiveTimestamp.getDate()), CLOCK_SKEW_TOLERATED_IN_HOURS)) {

            LOGGER.error("error=clock-sync device_id={} origin_time={} received_time={}", deviceId, formatter.format(ntpOriginTimestamp.getDate()), formatter.format(ntpReceiveTimestamp.getDate()));
            senseClockOutOfSync.mark(1);
        }

        final Ntp.NTPDataPacket.Builder ntpDataPacketBuilder = Ntp.NTPDataPacket.newBuilder()
            .setReferenceTs(data.getReferenceTs())
            .setOriginTs(data.getOriginTs())
            .setReceiveTs(ntpReceiveTimestamp.ntpValue())
            .setTransmitTs(new TimeStamp(new Date()).ntpValue());

        final Ntp.NTPDataPacket ntpPacket = ntpDataPacketBuilder.build();

        LOGGER.debug("Len pb = {}", ntpPacket.toByteArray().length);

        final Optional<byte[]> signedResponse = SignedMessage.sign(ntpPacket.toByteArray(), optionalKeyBytes.get());

        if (!signedResponse.isPresent()) {
            LOGGER.error("error=fail_message_sign device_id={}", deviceId);
            return plainTextError(Response.Status.INTERNAL_SERVER_ERROR, "");
        }

        final int responseLength = signedResponse.get().length;
        if (responseLength > 2048) {
            LOGGER.warn("warn=response_size response_size={} device_id={}", responseLength, deviceId);
        }

        return signedResponse.get();
    }

    public Optional<byte[]> getKey(String deviceId, List<String> groups, String ipAddress) {

        if (KeyStoreDynamoDB.DEFAULT_FACTORY_DEVICE_ID.equals(deviceId) &&
                featureFlipper.deviceFeatureActive(FeatureFlipper.OFFICE_ONLY_OVERRIDE, deviceId, groups)) {
            if (ipAddress.equals(LOCAL_OFFICE_IP_ADDRESS)) {
                return keyStore.get(deviceId);
            } else {
                return keyStore.getStrict(deviceId);
            }
        }
        return keyStore.get(deviceId);

    }

    public static boolean isClockOutOfSync(final DateTime sampleTime, final DateTime referenceTime, final Integer offsetThreshold) {
        return sampleTime.isAfter(referenceTime.plusHours(offsetThreshold)) || sampleTime.isBefore(referenceTime.minusHours(offsetThreshold));
    }
}
