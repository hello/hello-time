package com.hello.time.modules;

import com.hello.suripu.core.db.FeatureStore;
import com.hello.suripu.core.flipper.DynamoDBAdapter;
import com.hello.suripu.coredw8.resources.BaseResource;
import com.hello.time.HelloTime;
import com.hello.time.resources.TimeResource;
import com.librato.rollout.RolloutAdapter;
import com.librato.rollout.RolloutClient;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = {
        TimeResource.class,
        BaseResource.class,
        HelloTime.class
})
public class RolloutModule {
    private final FeatureStore featureStore;
    private final Integer pollingIntervalInSeconds;

    public RolloutModule(final FeatureStore featureStore, final Integer pollingIntervalInSeconds) {
        this.featureStore = featureStore;
        this.pollingIntervalInSeconds = pollingIntervalInSeconds;
    }

    @Provides @Singleton
    RolloutAdapter providesRolloutAdapter() {
        return new DynamoDBAdapter(featureStore, pollingIntervalInSeconds);
    }

    @Provides @Singleton
    RolloutClient providesRolloutClient(RolloutAdapter adapter) {
        return new RolloutClient(adapter);
    }
}
