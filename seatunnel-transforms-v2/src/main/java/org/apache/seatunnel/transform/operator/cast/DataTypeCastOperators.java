package org.apache.seatunnel.transform.operator.cast;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

public class DataTypeCastOperators {
    public static final DataTypeCastOperator DEFAULT_OPERATOR = e -> e;

    public static <T, R> DataTypeCastOperator<T, R> get(SeaTunnelDataType<T> inputType,
                                                        SeaTunnelDataType<R> outputType) {
        switch (inputType.getSqlType()) {
            case STRING:
                return (DataTypeCastOperator<T, R>) StringCastOperators.create(outputType);
            case BOOLEAN:
                return (DataTypeCastOperator<T, R>) BooleanCastOperators.create(outputType);
            case TINYINT:
                return (DataTypeCastOperator<T, R>) ByteCastOperators.create(outputType);
            case SMALLINT:
                return (DataTypeCastOperator<T, R>) ShortCastOperators.create(outputType);
            case INT:
                return (DataTypeCastOperator<T, R>) IntegerCastOperators.create(outputType);
            case BIGINT:
                return (DataTypeCastOperator<T, R>) LongCastOperators.create(outputType);
            case FLOAT:
                return (DataTypeCastOperator<T, R>) FloatCastOperators.create(outputType);
            case DOUBLE:
                return (DataTypeCastOperator<T, R>) DoubleCastOperators.create(outputType);
            case DECIMAL:
                return (DataTypeCastOperator<T, R>) DecimalCastOperators.create(outputType);
            case DATE:
                return (DataTypeCastOperator<T, R>) LocalDateCastOperators.create(outputType);
            case TIME:
                return (DataTypeCastOperator<T, R>) LocalTimeCastOperators.create(outputType);
            case TIMESTAMP:
                return (DataTypeCastOperator<T, R>) LocalDateTimeCastOperators.create(outputType);
            case ARRAY:
                return (DataTypeCastOperator<T, R>) ArrayCastOperators.create(outputType);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
