package com.github.kyleburton.teporingo;

/**
 * Exception type to represent that a breaker is in an open state.
 * This is thrown when a call to a wrapped function is not made
 * because the breaker is open.
 *
 * @author Kyle Burton <kyle.burton@gmail.com>
 */

public class MaxPublishRetriesExceededException extends Exception {
  public Throwable [] errors;
  public BreakerOpenException(String msg, Throwable [] theErrors) {
    errors = theErrors;
    super(msg);
  }

  public BreakerOpenException(String msg,Throwable cause, Throwable [] theErrors) {
    errors = theErrors;
    super(msg,cause);
  }
}
