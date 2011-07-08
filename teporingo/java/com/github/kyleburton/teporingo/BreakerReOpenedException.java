package com.github.kyleburton.teporingo;

/**
 * Exception type to represent the failure of an initial re-try when a
 * breaker is open and testing the wrapped function again.  This is
 * thrown when a wrapped function throws an exception (fails), the
 * inner exception will be captured as the cause on the
 * BreakerOpenedException.
 *
 * @author Kyle Burton <kyle.burton@gmail.com>
 */
public class BreakerReOpenedException extends BreakerOpenException {
  public BreakerReOpenedException(String msg) {
    super(msg);
  }

  public BreakerReOpenedException(String msg,Throwable cause) {
    super(msg,cause);
  }
}
