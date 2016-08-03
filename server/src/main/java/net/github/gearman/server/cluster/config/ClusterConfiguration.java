package net.github.gearman.server.cluster.config;

import com.hazelcast.core.HazelcastInstance;

public class ClusterConfiguration {

    private HazelcastConfiguration hazelcastConfiguration;

    public ClusterConfiguration(){
    }

    public ClusterConfiguration(HazelcastConfiguration config){
        this.hazelcastConfiguration = config;
    }

    public void setHazelcast(HazelcastConfiguration hazelcastConfiguration) {
        this.hazelcastConfiguration = hazelcastConfiguration;
    }

    public HazelcastConfiguration getHazelcast() {
        return hazelcastConfiguration;
    }

    public HazelcastInstance getHazelcastInstance() {
        return getHazelcast().getHazelcastInstance();
    }
}
