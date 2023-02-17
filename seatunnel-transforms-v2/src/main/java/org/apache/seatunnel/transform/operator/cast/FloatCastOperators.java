package org.apache.seatunnel.transform.operator.cast;

import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.FLOAT_1;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.math.BigDecimal;

public class FloatCastOperators {

    public static <R> DataTypeCastOperator<Float, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BOOLEAN:
                return (DataTypeCastOperator<Float, R>) (DataTypeCastOperator<Float, Boolean>) b -> b.equals(FLOAT_1) ? Boolean.TRUE : Boolean.FALSE;
            case TINYINT:
                return (DataTypeCastOperator<Float, R>) (DataTypeCastOperator<Float, Byte>) b -> b.byteValue();
            case SMALLINT:
                return (DataTypeCastOperator<Float, R>) (DataTypeCastOperator<Float, Short>) b -> b.shortValue();
            case INT:
                return (DataTypeCastOperator<Float, R>) (DataTypeCastOperator<Float, Integer>) b -> b.intValue();
            case BIGINT:
                return (DataTypeCastOperator<Float, R>) (DataTypeCastOperator<Float, Long>) b -> b.longValue();
            case DOUBLE:
                return (DataTypeCastOperator<Float, R>) (DataTypeCastOperator<Float, Double>) b -> b.doubleValue();
            case DECIMAL:
                return (DataTypeCastOperator<Float, R>) (DataTypeCastOperator<Float, BigDecimal>) b -> new BigDecimal(b);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
