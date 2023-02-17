package org.apache.seatunnel.transform.operator.cast;

import java.io.Serializable;

@FunctionalInterface
public interface DataTypeCastOperator<T, R> extends Serializable {
    R cast(T t);
}
