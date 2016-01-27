package com.hello.time;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;

import com.google.common.collect.ImmutableMap;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.hello.dropwizard.mikkusu.helpers.JacksonProtobufProvider;
import com.hello.dropwizard.mikkusu.resources.PingResource;
import com.hello.dropwizard.mikkusu.resources.VersionResource;
import com.hello.suripu.core.ObjectGraphRoot;
import com.hello.suripu.core.db.TeamStore;
import com.hello.suripu.core.db.util.JodaArgumentFactory;
import com.hello.suripu.core.db.util.PostgresIntegerArrayArgumentFactory;
import com.hello.suripu.core.flipper.DynamoDBAdapter;
import com.hello.suripu.core.flipper.GroupFlipper;
import com.hello.suripu.core.oauth.stores.PersistentApplicationStore;
import com.hello.suripu.coredw8.filters.SlowRequestsFilter;
import com.hello.suripu.coredw8.db.AccessTokenDAO;
import com.hello.suripu.coredw8.health.DynamoDbHealthCheck;
import com.hello.suripu.coredw8.health.KinesisHealthCheck;
import com.hello.suripu.coredw8.clients.AmazonDynamoDBClientFactory;
import com.hello.suripu.core.configuration.DynamoDBTableName;
import com.hello.suripu.core.configuration.QueueName;
import com.hello.suripu.core.db.ApplicationsDAO;
import com.hello.suripu.core.db.FeatureStore;
import com.hello.suripu.core.db.KeyStore;
import com.hello.suripu.core.db.KeyStoreDynamoDB;
import com.hello.suripu.core.logging.DataLogger;
import com.hello.suripu.core.logging.KinesisLoggerFactory;
import com.hello.suripu.coredw8.managers.DynamoDBClientManaged;
import com.hello.suripu.coredw8.managers.KinesisClientManaged;
import com.hello.suripu.coredw8.oauth.AccessToken;
import com.hello.suripu.coredw8.oauth.AuthDynamicFeature;
import com.hello.suripu.coredw8.oauth.AuthValueFactoryProvider;
import com.hello.suripu.coredw8.oauth.OAuthAuthenticator;
import com.hello.suripu.coredw8.oauth.OAuthAuthorizer;
import com.hello.suripu.coredw8.oauth.OAuthCredentialAuthFilter;
import com.hello.suripu.coredw8.oauth.ScopesAllowedDynamicFeature;
import com.hello.suripu.coredw8.oauth.stores.PersistentAccessTokenStore;
import com.hello.suripu.coredw8.util.CustomJSONExceptionMapper;
import com.hello.time.configuration.SuripuConfiguration;
import com.hello.time.modules.RolloutModule;
import com.hello.time.resources.TimeResource;
import com.librato.rollout.RolloutClient;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;

import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.jdbi.OptionalContainerFactory;
import io.dropwizard.jdbi.bundles.DBIExceptionsBundle;
import io.dropwizard.Application;
import io.dropwizard.server.AbstractServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class HelloTime extends Application<SuripuConfiguration> {

  private final static Logger LOGGER = LoggerFactory.getLogger(HelloTime.class);

  public static void main(final String[] args) throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    DateTimeZone.setDefault(DateTimeZone.UTC);
    new HelloTime().run(args);
  }

  @Override
  public void initialize(Bootstrap<SuripuConfiguration> bootstrap) {
    bootstrap.addBundle(new DBIExceptionsBundle());
  }

  @Override
  public void run(final SuripuConfiguration configuration, Environment environment) throws Exception {
    environment.jersey().register(new JacksonProtobufProvider());

    final DBIFactory factory = new DBIFactory();
    final DBI commonDB = factory.build(environment, configuration.getCommonDB(), "postgresql");

    commonDB.registerArgumentFactory(new JodaArgumentFactory());
    commonDB.registerContainerFactory(new OptionalContainerFactory());
    commonDB.registerArgumentFactory(new PostgresIntegerArrayArgumentFactory());

    // Checks Environment first and then instance profile.
    final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();

    final ImmutableMap<DynamoDBTableName, String> tableNames = configuration.dynamoDBConfiguration().tables();
    final ClientConfiguration clientConfig = new ClientConfiguration().withConnectionTimeout(200).withMaxErrorRetry(1).withMaxConnections(100);
    final AmazonDynamoDBClientFactory dynamoDBFactory = AmazonDynamoDBClientFactory.create(awsCredentialsProvider, clientConfig, configuration.dynamoDBConfiguration());

    final AmazonDynamoDB senseKeyStoreDynamoDBClient = dynamoDBFactory.getForTable(DynamoDBTableName.SENSE_KEY_STORE);

    final ClientConfiguration kinesisClientConfiguration = new ClientConfiguration().withMaxConnections(100);
    final AmazonKinesisAsyncClient kinesisClient = new AmazonKinesisAsyncClient(awsCredentialsProvider, kinesisClientConfiguration);
    kinesisClient.setEndpoint(configuration.getKinesisConfiguration().getEndpoint());

    final KinesisLoggerFactory kinesisLoggerFactory = new KinesisLoggerFactory(
        kinesisClient,
        configuration.getKinesisConfiguration().getStreams()
    );

    final KeyStore senseKeyStore = new KeyStoreDynamoDB(
        senseKeyStoreDynamoDBClient,
        tableNames.get(DynamoDBTableName.SENSE_KEY_STORE),
        "1234567891234567".getBytes(), // TODO: REMOVE THIS WHEN WE ARE NOT SUPPOSED TO HAVE A DEFAULT KEY
        120 // 2 minutes for cache
    );

    if (configuration.getMetricsEnabled()) {
      final String graphiteHostName = configuration.getGraphite().getHost();
      final String apiKey = configuration.getGraphite().getApiKey();
      final Integer interval = configuration.getGraphite().getReportingIntervalInSeconds();
      final String env = (configuration.getDebug()) ? "dev" : "prod";
      final String prefix = String.format("%s.%s.hello-time", apiKey, env);

      final Graphite graphite = new Graphite(new InetSocketAddress(graphiteHostName, 2003));

      final GraphiteReporter reporter = GraphiteReporter.forRegistry(environment.metrics())
          .prefixedWith(prefix)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .filter(MetricFilter.ALL)
          .build(graphite);
      reporter.start(interval, TimeUnit.SECONDS);

      LOGGER.info("Metrics enabled.");
    } else {
      LOGGER.warn("Metrics not enabled.");
    }

    //Doing this programmatically instead of in config files
    AbstractServerFactory sf = (AbstractServerFactory) configuration.getServerFactory();
    // disable all default exception mappers
    sf.setRegisterDefaultExceptionMappers(false);

    environment.jersey().register(new CustomJSONExceptionMapper(configuration.getDebug()));

    final AccessTokenDAO accessTokenDAO = commonDB.onDemand(AccessTokenDAO.class);
    final ApplicationsDAO applicationsDAO = commonDB.onDemand(ApplicationsDAO.class);
    final PersistentApplicationStore applicationStore = new PersistentApplicationStore(applicationsDAO);
    final PersistentAccessTokenStore tokenStore = new PersistentAccessTokenStore(accessTokenDAO, applicationStore);
    final DataLogger activityLogger = kinesisLoggerFactory.get(QueueName.ACTIVITY_STREAM);
    final String namespace = (configuration.getDebug()) ? "dev" : "prod";
    final AmazonDynamoDB featuresDynamoDBClient = dynamoDBFactory.getForTable(DynamoDBTableName.FEATURES);
    final FeatureStore featureStore = new FeatureStore(featuresDynamoDBClient, tableNames.get(DynamoDBTableName.FEATURES), namespace);
    final AmazonDynamoDB teamStoreDynamoDBClient = dynamoDBFactory.getForTable(DynamoDBTableName.TEAMS);
    final TeamStore teamStore = new TeamStore(teamStoreDynamoDBClient, tableNames.get(DynamoDBTableName.TEAMS));
    final GroupFlipper groupFlipper = new GroupFlipper(teamStore, 30);

    environment.jersey().register(new AuthDynamicFeature(new OAuthCredentialAuthFilter.Builder<AccessToken>()
        .setAuthenticator(new OAuthAuthenticator(tokenStore))
        .setAuthorizer(new OAuthAuthorizer())
        .setRealm("SUPER SECRET STUFF")
        .setPrefix("Bearer")
        .setLogger(activityLogger)
        .buildAuthFilter()));
    environment.jersey().register(ScopesAllowedDynamicFeature.class);
    environment.jersey().register(new AuthValueFactoryProvider.Binder<>(AccessToken.class));

    final RolloutModule module = new RolloutModule(featureStore, 30);
    ObjectGraphRoot.getInstance().init(module);
    environment.jersey().register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(new RolloutClient(new DynamoDBAdapter(featureStore, 30))).to(RolloutClient.class);
      }
    });

    final TimeResource timeResource = new TimeResource(
        senseKeyStore,
        kinesisLoggerFactory,
        groupFlipper,
        environment.metrics(),
        configuration.getDebug()
    );

    environment.jersey().register(timeResource);
    environment.jersey().register(new PingResource());
    environment.jersey().register(new VersionResource());

    final FilterRegistration.Dynamic builder = environment.servlets().addFilter("slowRequestsFilter", SlowRequestsFilter.class);
    builder.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

    // Manage the lifecycle of our clients
    environment.lifecycle().manage(new DynamoDBClientManaged(senseKeyStoreDynamoDBClient));
    environment.lifecycle().manage(new DynamoDBClientManaged(featuresDynamoDBClient));
    environment.lifecycle().manage(new KinesisClientManaged(kinesisClient));

    // Make sure we can connect
    environment.healthChecks().register("keystore-healthcheck", new DynamoDbHealthCheck(senseKeyStoreDynamoDBClient));
    environment.healthChecks().register("features-healthcheck", new DynamoDbHealthCheck(featuresDynamoDBClient));
    environment.healthChecks().register("kinesis-healthcheck", new KinesisHealthCheck(kinesisClient));
  }


}
