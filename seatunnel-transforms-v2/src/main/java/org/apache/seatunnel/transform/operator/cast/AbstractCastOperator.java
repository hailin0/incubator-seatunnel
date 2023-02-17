package org.apache.seatunnel.transform.operator.cast;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractCastOperator<T, R> implements DataTypeCastOperator<T, R> {
    private final SeaTunnelDataType<T> inputType;
    private final SeaTunnelDataType<R> outputType;
}
