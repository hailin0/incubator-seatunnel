package org.apache.seatunnel.transform.operator.cast;

import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.LONG_1;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.math.BigDecimal;

public class LongCastOperators {

    public static <R> DataTypeCastOperator<Long, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BOOLEAN:
                return (DataTypeCastOperator<Long, R>) (DataTypeCastOperator<Long, Boolean>) b -> b.equals(LONG_1) ? Boolean.TRUE : Boolean.FALSE;
            case TINYINT:
                return (DataTypeCastOperator<Long, R>) (DataTypeCastOperator<Long, Byte>) b -> b.byteValue();
            case SMALLINT:
                return (DataTypeCastOperator<Long, R>) (DataTypeCastOperator<Long, Short>) b -> b.shortValue();
            case INT:
                return (DataTypeCastOperator<Long, R>) (DataTypeCastOperator<Long, Integer>) b -> b.intValue();
            case FLOAT:
                return (DataTypeCastOperator<Long, R>) (DataTypeCastOperator<Long, Float>) b -> b.floatValue();
            case DOUBLE:
                return (DataTypeCastOperator<Long, R>) (DataTypeCastOperator<Long, Double>) b -> b.doubleValue();
            case DECIMAL:
                return (DataTypeCastOperator<Long, R>) (DataTypeCastOperator<Long, BigDecimal>) b -> new BigDecimal(b);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
