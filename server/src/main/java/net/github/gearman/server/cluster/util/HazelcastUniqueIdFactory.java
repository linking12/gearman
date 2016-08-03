package net.github.gearman.server.cluster.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;

import net.github.gearman.engine.core.UniqueIdFactory;

public class HazelcastUniqueIdFactory implements UniqueIdFactory {

    private final IdGenerator idGenerator;

    public HazelcastUniqueIdFactory(HazelcastInstance hazelcast){
        idGenerator = hazelcast.getIdGenerator("job-ids");
    }

    @Override
    public String generateUniqueId() {
        return String.valueOf(idGenerator.newId());
    }
}
