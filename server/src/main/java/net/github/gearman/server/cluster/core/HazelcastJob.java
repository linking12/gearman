package net.github.gearman.server.cluster.core;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import net.github.gearman.common.Job;

public class HazelcastJob extends Job implements DataSerializable {

    private static final long serialVersionUID = 1L;

    public HazelcastJob(){
    }

    public HazelcastJob(Job job){
        cloneOtherJob(job);
    }

    public Job toJob() {
        return new Job(this);
    }

    @Override
    public void writeData(ObjectDataOutput output) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        output.writeUTF(mapper.writeValueAsString(this));
    }

    @Override
    public void readData(ObjectDataInput input) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Job job = mapper.readValue(input.readUTF(), this.getClass());
        this.cloneOtherJob(job);
    }
}
