package net.github.gearman.engine.exceptions;

/**
 * 初始化异常
 * @author tianyao.myc
 *
 */
public class InitException extends Exception {

	/** 序列化ID */
	private static final long serialVersionUID = 3470539219990355580L;

	public InitException() {
		super();
	}
	
	public InitException(String message) {
		super(message);
	}
	
	public InitException(Throwable error) {
		super(error);
	}
	
	public InitException(String message, Throwable error) {
		super(message, error);
	}
}
