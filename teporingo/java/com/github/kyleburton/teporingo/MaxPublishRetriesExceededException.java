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
  public MaxPublishRetriesExceededException(String msg, Throwable [] theErrors) {
    super(msg);
    errors = theErrors;
  }

  public MaxPublishRetriesExceededException(String msg,Throwable cause, Throwable [] theErrors) {
    super(msg,cause);
    errors = theErrors;
  }
}
