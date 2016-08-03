package net.github.gearman.engine.queue;

import java.util.Collection;

import com.google.common.collect.ImmutableMap;

import net.github.gearman.common.Job;
import net.github.gearman.constants.JobPriority;
import net.github.gearman.engine.core.QueuedJob;
import net.github.gearman.engine.exceptions.PersistenceException;
import net.github.gearman.engine.exceptions.QueueFullException;

public interface JobQueue {

    /**
     * Enqueue work
     * 
     * @param job
     */
    void enqueue(Job job) throws QueueFullException, PersistenceException;

    /**
     * Fetch the next job waiting -- this checks high, then normal, then low Caveat: in the normal queue, we skip over
     * any jobs whose timestamp has not come yet (support for epoch jobs)
     *
     * @return Next Job in the queue, null if none
     */
    Job poll();

    /**
     * Remove a job from the queue - simply deleting it
     * 
     * @param job
     * @return true on success, false otherwise
     */
    boolean remove(Job job);

    /**
     * Determine if the unique ID specified is in use.
     * 
     * @param uniqueID The job's unique ID
     * @return true or false.
     */
    boolean uniqueIdInUse(String uniqueID);

    long size(JobPriority priority);

    boolean isEmpty();

    void setCapacity(int size);

    String getName();

    long size();

    // Data
    Collection<QueuedJob> getAllJobs();

    Job findJobByUniqueId(String uniqueID);

    ImmutableMap<Integer, Long> futureCounts();
}
