package org.apache.seatunnel.transform.operator.cast;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.math.BigDecimal;

public class StringCastOperators {

    public static <R> DataTypeCastOperator<String, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BOOLEAN:
                return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Boolean>) s -> Boolean.valueOf(s);
            case TINYINT:
                return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Byte>) s -> Byte.valueOf(s);
            case SMALLINT:
                return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Short>) s -> Short.valueOf(s);
            case INT:
                return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Integer>) s -> Integer.valueOf(s);
            case BIGINT:
                return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Long>) s -> Long.valueOf(s);
            case FLOAT:
                return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Float>) s -> Float.valueOf(s);
            case DOUBLE:
                return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Double>) s -> Double.valueOf(s);
            case DECIMAL:
                return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, BigDecimal>) s -> new BigDecimal(s);
            case ARRAY:
                ArrayType arrayType = (ArrayType) outputType;
                switch (arrayType.getElementType().getSqlType()) {
                    case STRING:
                        return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, String[]>) s -> {
                            return new String[0];
                        };
                    case BOOLEAN:
                        return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Boolean[]>) s -> {
                            return new Boolean[0];
                        };
                    case TINYINT:
                        return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Byte[]>) s -> {
                            return new Byte[0];
                        };
                    case SMALLINT:
                        return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Short[]>) s -> {
                            return new Short[0];
                        };
                    case INT:
                        return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Integer[]>) s -> {
                            return new Integer[0];
                        };
                    case BIGINT:
                        return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Long[]>) s -> {
                            return new Long[0];
                        };
                    case FLOAT:
                        return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Float[]>) s -> {
                            return new Float[0];
                        };
                    case DOUBLE:
                        return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, Double[]>) s -> {
                            return new Double[0];
                        };
                    case DECIMAL:
                        return (DataTypeCastOperator<String, R>) (DataTypeCastOperator<String, BigDecimal[]>) s -> {
                            return new BigDecimal[0];
                        };
                    default:
                        throw new UnsupportedOperationException();
                }
            default:
                throw new UnsupportedOperationException();
        }
    }
}
