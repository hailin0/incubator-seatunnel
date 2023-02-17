package org.apache.seatunnel.transform.operator.cast;

import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.DOUBLE_1;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.math.BigDecimal;

public class DoubleCastOperators {

    public static <R> DataTypeCastOperator<Double, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BOOLEAN:
                return (DataTypeCastOperator<Double, R>) (DataTypeCastOperator<Double, Boolean>) b -> b.equals(DOUBLE_1) ? Boolean.TRUE : Boolean.FALSE;
            case TINYINT:
                return (DataTypeCastOperator<Double, R>) (DataTypeCastOperator<Double, Byte>) b -> b.byteValue();
            case SMALLINT:
                return (DataTypeCastOperator<Double, R>) (DataTypeCastOperator<Double, Short>) b -> b.shortValue();
            case INT:
                return (DataTypeCastOperator<Double, R>) (DataTypeCastOperator<Double, Integer>) b -> b.intValue();
            case BIGINT:
                return (DataTypeCastOperator<Double, R>) (DataTypeCastOperator<Double, Long>) b -> b.longValue();
            case FLOAT:
                return (DataTypeCastOperator<Double, R>) (DataTypeCastOperator<Double, Float>) b -> b.floatValue();
            case DECIMAL:
                return (DataTypeCastOperator<Double, R>) (DataTypeCastOperator<Double, BigDecimal>) b -> new BigDecimal(b);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
