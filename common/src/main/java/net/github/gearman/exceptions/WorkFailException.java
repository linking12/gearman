package net.github.gearman.exceptions;

public class WorkFailException extends WorkException {

    private static final long serialVersionUID = 1L;

    public WorkFailException(final String jobHandle){
        this.jobHandle = jobHandle;
    }
}
