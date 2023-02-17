package org.apache.seatunnel.transform.operator.cast;

import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.DECIMAL_1;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.math.BigDecimal;

public class DecimalCastOperators {

    public static <R> DataTypeCastOperator<BigDecimal, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BOOLEAN:
                return (DataTypeCastOperator<BigDecimal, R>) (DataTypeCastOperator<BigDecimal, Boolean>) b -> b.equals(DECIMAL_1) ? Boolean.TRUE : Boolean.FALSE;
            case TINYINT:
                return (DataTypeCastOperator<BigDecimal, R>) (DataTypeCastOperator<BigDecimal, Byte>) b -> b.byteValue();
            case SMALLINT:
                return (DataTypeCastOperator<BigDecimal, R>) (DataTypeCastOperator<BigDecimal, Short>) b -> b.shortValue();
            case INT:
                return (DataTypeCastOperator<BigDecimal, R>) (DataTypeCastOperator<BigDecimal, Integer>) b -> b.intValue();
            case BIGINT:
                return (DataTypeCastOperator<BigDecimal, R>) (DataTypeCastOperator<BigDecimal, Long>) b -> b.longValue();
            case FLOAT:
                return (DataTypeCastOperator<BigDecimal, R>) (DataTypeCastOperator<BigDecimal, Float>) b -> b.floatValue();
            case DOUBLE:
                return (DataTypeCastOperator<BigDecimal, R>) (DataTypeCastOperator<BigDecimal, Double>) b -> b.doubleValue();
            default:
                throw new UnsupportedOperationException();
        }
    }
}
