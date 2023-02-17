package org.apache.seatunnel.transform.operator.cast;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.math.BigDecimal;

public class ByteCastOperators {
    static final Byte BYTE_0 = 0;
    static final Byte BYTE_1 = 1;
    static final Short SHORT_0 = Short.valueOf(BYTE_0);
    static final Short SHORT_1 = Short.valueOf(BYTE_1);
    static final Integer INTEGER_0 = Integer.valueOf(BYTE_0);
    static final Integer INTEGER_1 = Integer.valueOf(BYTE_1);
    static final Long LONG_0 = Long.valueOf(BYTE_0);
    static final Long LONG_1 = Long.valueOf(BYTE_1);
    static final Float FLOAT_0 = Float.valueOf(BYTE_0);
    static final Float FLOAT_1 = Float.valueOf(BYTE_1);
    static final Double DOUBLE_0 = Double.valueOf(BYTE_0);
    static final Double DOUBLE_1 = Double.valueOf(BYTE_1);
    static final BigDecimal DECIMAL_0 = BigDecimal.valueOf(BYTE_0);
    static final BigDecimal DECIMAL_1 = BigDecimal.valueOf(BYTE_1);

    public static <R> DataTypeCastOperator<Byte, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BOOLEAN:
                return (DataTypeCastOperator<Byte, R>) (DataTypeCastOperator<Byte, Boolean>) b -> b.equals(BYTE_1) ? Boolean.TRUE : Boolean.FALSE;
            case SMALLINT:
                return (DataTypeCastOperator<Byte, R>) (DataTypeCastOperator<Byte, Short>) b -> b.equals(BYTE_1) ? SHORT_1 : SHORT_0;
            case INT:
                return (DataTypeCastOperator<Byte, R>) (DataTypeCastOperator<Byte, Integer>) b -> b.equals(BYTE_1) ? INTEGER_1 : INTEGER_0;
            case BIGINT:
                return (DataTypeCastOperator<Byte, R>) (DataTypeCastOperator<Byte, Long>) b -> b.equals(BYTE_1) ? LONG_1 : LONG_0;
            case FLOAT:
                return (DataTypeCastOperator<Byte, R>) (DataTypeCastOperator<Byte, Float>) b -> b.equals(BYTE_1) ? FLOAT_1 : FLOAT_0;
            case DOUBLE:
                return (DataTypeCastOperator<Byte, R>) (DataTypeCastOperator<Byte, Double>) b -> b.equals(BYTE_1) ? DOUBLE_1 : DOUBLE_0;
            case DECIMAL:
                return (DataTypeCastOperator<Byte, R>) (DataTypeCastOperator<Byte, BigDecimal>) b -> b.equals(BYTE_1) ? DECIMAL_1 : DECIMAL_0;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
