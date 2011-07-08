package com.github.kyleburton.teporingo;

/**
 * Exception type to represent a breaker entering an opened state.
 * This is thrown when a wrapped function throws an exception (fails),
 * the inner exception will be captured as the cause on the
 * BreakerOpenedException.
 *
 * @author Kyle Burton <kyle.burton@gmail.com>
 */
public class BreakerOpenedException extends BreakerOpenException {
  public BreakerOpenedException(String msg) {
    super(msg);
  }

  public BreakerOpenedException(String msg,Throwable cause) {
    super(msg,cause);
  }
}
