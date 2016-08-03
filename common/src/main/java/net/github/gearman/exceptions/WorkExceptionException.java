package net.github.gearman.exceptions;

public class WorkExceptionException extends WorkException {

    private static final long serialVersionUID = 1L;
    private String            message;

    public WorkExceptionException(final String jobHandle, final byte[] exceptionData){
        this.jobHandle = jobHandle;
        this.message = new String(exceptionData);
    }

    public String getMessage() {
        return message;
    }
}
