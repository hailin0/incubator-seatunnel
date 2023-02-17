package org.apache.seatunnel.transform.operator.cast;

import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.INTEGER_1;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.math.BigDecimal;

public class IntegerCastOperators {

    public static <R> DataTypeCastOperator<Integer, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BOOLEAN:
                return (DataTypeCastOperator<Integer, R>) (DataTypeCastOperator<Integer, Boolean>) b -> b.equals(INTEGER_1) ? Boolean.TRUE : Boolean.FALSE;
            case TINYINT:
                return (DataTypeCastOperator<Integer, R>) (DataTypeCastOperator<Integer, Byte>) b -> b.byteValue();
            case SMALLINT:
                return (DataTypeCastOperator<Integer, R>) (DataTypeCastOperator<Integer, Short>) b -> b.shortValue();
            case BIGINT:
                return (DataTypeCastOperator<Integer, R>) (DataTypeCastOperator<Integer, Long>) b -> b.longValue();
            case FLOAT:
                return (DataTypeCastOperator<Integer, R>) (DataTypeCastOperator<Integer, Float>) b -> b.floatValue();
            case DOUBLE:
                return (DataTypeCastOperator<Integer, R>) (DataTypeCastOperator<Integer, Double>) b -> b.doubleValue();
            case DECIMAL:
                return (DataTypeCastOperator<Integer, R>) (DataTypeCastOperator<Integer, BigDecimal>) b -> new BigDecimal(b);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
