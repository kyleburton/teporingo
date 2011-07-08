package com.github.kyleburton.teporingo;

/**
 * Exception type to represent that a breaker is in an open state.
 * This is thrown when a call to a wrapped function is not made
 * because the breaker is open.
 *
 * @author Kyle Burton <kyle.burton@gmail.com>
 */

public class BreakerOpenException extends Exception {
  public BreakerOpenException(String msg) {
    super(msg);
  }

  public BreakerOpenException(String msg,Throwable cause) {
    super(msg,cause);
  }
}
