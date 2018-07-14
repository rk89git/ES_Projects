/*
 * 
 */
package com.db.common.exception;

import java.io.Serializable;

/**
 * @y.exclude A runtime exception class that allows to wrap all exceptions with
 *            custom error code and description.
 *            <p>
 *            This class helps to manage the exception handling in much easier
 *            way since all exceptions are wrapped using this.
 *            </p>
 *            <p>
 *            The exceptions can also wrap their exceptions using the throwable
 *            occured when getting the exception.
 *            </p>
 * 
 * 
 */
public class DBAnalyticsException extends RuntimeException implements Serializable {

	private static final long serialVersionUID = 6046114549369556760L;

	private final String errorId;

	/**
	 * Instantiates the exception with custom message.
	 * 
	 * @param message
	 *            - the message for the exception occurred.
	 */
	public DBAnalyticsException(String message) {
		this(message, (String) null);
	}

	/**
	 * Instantiates the exception with custom message and error code.
	 * 
	 * @param message
	 *            - the message for the exception occurred.
	 * @param errorId
	 *            - the error code for the exception occurred.
	 */
	public DBAnalyticsException(String message, String errorId) {
		super(message);
		this.errorId = errorId;
	}

	/**
	 * Instantiates the exception with throwable.
	 * 
	 * @param cause
	 *            - the throwable instance produced by java exception class.
	 */
	public DBAnalyticsException(Throwable cause) {
		this(cause, (String) null);

	}

	/**
	 * Instantiates the exception with throwable and custom error code.
	 * 
	 * @param cause
	 *            - the throwable instance produced by java exception class.
	 * @param errorId
	 *            - the error code for the exception occurred.
	 */
	public DBAnalyticsException(Throwable cause, String errorId) {
		super(cause);
		this.errorId = errorId;
	}

	/**
	 * Instantiates the exception with throwable and custom error message.
	 * 
	 * @param message
	 *            - the message for the exception occurred.
	 * @param cause
	 *            - the throwable instance produced by java exception class.
	 */
	public DBAnalyticsException(String message, Throwable cause) {
		this(message, cause, null);
	}

	/**
	 * Instantiates the exception with throwable,custom error code and custom
	 * error message.
	 * 
	 * @param message
	 *            - the message for the exception occurred.
	 * @param cause
	 *            - the throwable instance produced by java exception class.
	 * @param errorId
	 *            - the error code for the exception occurred.
	 */
	public DBAnalyticsException(String message, Throwable cause, String errorId) {
		super(message, cause);
		this.errorId = errorId;
	}

	/**
	 * returns the error code of the exception.
	 * 
	 * @return - the error code.
	 */
	public String getErrorId() {
		return errorId;
	}
}
