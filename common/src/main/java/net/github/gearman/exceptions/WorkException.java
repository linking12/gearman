package net.github.gearman.exceptions;

public class WorkException extends Throwable {

    private static final long serialVersionUID = 1L;
    protected String          jobHandle;

    public String getJobHandle() {
        return jobHandle;
    }
}
