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
import com.hello.suripu.core.logging.KinesisLoggerFactory;
import com.hello.suripu.core.resources.BaseResource;
import com.hello.suripu.core.util.HelloHttpHeader;
import com.hello.suripu.core.util.SignedMessage;
import com.librato.rollout.RolloutClient;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.net.ntp.TimeStamp;
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
import java.util.Date;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;


@Path("/")
public class TimeResource extends BaseResource {

    @Inject
    RolloutClient featureFlipper;

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeResource.class);
    private static final String LOCAL_OFFICE_IP_ADDRESS = "199.87.82.114";
    private static final String FIRMWARE_DEFAULT = "0";

    private final KeyStore keyStore;

    private final KinesisLoggerFactory kinesisLoggerFactory;

    private final GroupFlipper groupFlipper;

    private final MetricRegistry metrics;
    protected Meter senseClockOutOfSync;
    protected Histogram drift;

    @Context
    HttpServletRequest request;

    public TimeResource(final KeyStore keyStore,
                        final KinesisLoggerFactory kinesisLoggerFactory,
                        final GroupFlipper groupFlipper,
                        final MetricRegistry metricRegistry) {

        this.keyStore = keyStore;
        this.kinesisLoggerFactory = kinesisLoggerFactory;

        this.metrics= metricRegistry;
        this.groupFlipper = groupFlipper;
        //TODO: Figure out what metrics are valuable and update these.
        this.senseClockOutOfSync = metrics.meter(name(TimeResource.class, "sense-clock-out-sync"));
        this.drift = metrics.histogram(name(TimeResource.class, "sense-drift"));
    }


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

        final String topFW = (this.request.getHeader(HelloHttpHeader.TOP_FW_VERSION) != null) ? this.request.getHeader(HelloHttpHeader.TOP_FW_VERSION) : FIRMWARE_DEFAULT;
        final String middleFW = (this.request.getHeader(HelloHttpHeader.MIDDLE_FW_VERSION) != null) ? this.request.getHeader(HelloHttpHeader.MIDDLE_FW_VERSION) : FIRMWARE_DEFAULT;

        LOGGER.debug("action=request-time device_id = {}", debugSenseId);

        Ntp.NTPDataPacket data = null;

        try {
            data = Ntp.NTPDataPacket.parseFrom(signedMessage.body);
        } catch (IOException exception) {
            final String errorMessage = String.format("Failed parsing protobuf for deviceId = %s : %s", debugSenseId, exception.getMessage());
            LOGGER.error(errorMessage);
            return plainTextError(Response.Status.BAD_REQUEST, "bad request");
        }
        LOGGER.debug("Received valid protobuf {}", data.toString());
        LOGGER.debug("Received protobuf message {}", TextFormat.shortDebugString(data));

        if (!data.hasReferenceTs()) {
            LOGGER.error("error=empty-reference_ts");
            return plainTextError(Response.Status.BAD_REQUEST, "empty reference ts");
        }


        final String deviceId = debugSenseId;
        final List<String> groups = groupFlipper.getGroups(deviceId);
        final String ipAddress = getIpAddress(request);
        final List<String> ipGroups = groupFlipper.getGroups(ipAddress);


        if (featureFlipper.deviceFeatureActive(FeatureFlipper.PRINT_RAW_PB, deviceId, groups)) {
            LOGGER.debug("RAW_PB for device_id={} {}", deviceId, Hex.encodeHexString(body));
        }


        final Optional<byte[]> optionalKeyBytes = getKey(deviceId, groups, ipAddress);

        if (!optionalKeyBytes.isPresent()) {
            LOGGER.error("error=key-store-failure sense_id={}", deviceId);
            return plainTextError(Response.Status.BAD_REQUEST, "");
        }

        final Optional<SignedMessage.Error> error = signedMessage.validateWithKey(optionalKeyBytes.get());

        if (error.isPresent()) {
            LOGGER.error("{} sense_id={}", error.get().message, deviceId);
            return plainTextError(Response.Status.UNAUTHORIZED, "");
        }
        LOGGER.debug("{}", ntpReceiveTimestamp.toDateString());

        final Ntp.NTPDataPacket.Builder ntpDataPacketBuilder = Ntp.NTPDataPacket.newBuilder()
            .setReferenceTs(data.getReferenceTs())
            .setOriginTs(data.getOriginTs())
            .setReceiveTs(ntpReceiveTimestamp.ntpValue())
            .setTransmitTs(new TimeStamp(new Date()).ntpValue());

        final Ntp.NTPDataPacket ntpPacket = ntpDataPacketBuilder.build();

        LOGGER.debug("Len pb = {}", ntpPacket.toByteArray().length);

        final Optional<byte[]> signedResponse = SignedMessage.sign(ntpPacket.toByteArray(), optionalKeyBytes.get());

        if (!signedResponse.isPresent()) {
            LOGGER.error("Failed signing message");
            return plainTextError(Response.Status.INTERNAL_SERVER_ERROR, "");
        }

        final int responseLength = signedResponse.get().length;
        if (responseLength > 2048) {
            LOGGER.warn("response_size too large ({}) for device_id= {}", responseLength, deviceId);
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
}
