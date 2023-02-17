package org.apache.seatunnel.transform.operator.cast;

import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.SHORT_1;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.math.BigDecimal;

public class ShortCastOperators {

    public static <R> DataTypeCastOperator<Short, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BOOLEAN:
                return (DataTypeCastOperator<Short, R>) (DataTypeCastOperator<Short, Boolean>) b -> b.equals(SHORT_1) ? Boolean.TRUE : Boolean.FALSE;
            case TINYINT:
                return (DataTypeCastOperator<Short, R>) (DataTypeCastOperator<Short, Byte>) b -> b.byteValue();
            case INT:
                return (DataTypeCastOperator<Short, R>) (DataTypeCastOperator<Short, Integer>) b -> b.intValue();
            case BIGINT:
                return (DataTypeCastOperator<Short, R>) (DataTypeCastOperator<Short, Long>) b -> b.longValue();
            case FLOAT:
                return (DataTypeCastOperator<Short, R>) (DataTypeCastOperator<Short, Float>) b -> b.floatValue();
            case DOUBLE:
                return (DataTypeCastOperator<Short, R>) (DataTypeCastOperator<Short, Double>) b -> b.doubleValue();
            case DECIMAL:
                return (DataTypeCastOperator<Short, R>) (DataTypeCastOperator<Short, BigDecimal>) b -> new BigDecimal(b);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
