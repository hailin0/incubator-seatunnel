package org.apache.seatunnel.transform.operator.cast;

import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.BYTE_0;
import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.BYTE_1;
import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.INTEGER_0;
import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.INTEGER_1;
import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.LONG_0;
import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.LONG_1;
import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.SHORT_0;
import static org.apache.seatunnel.transform.operator.cast.ByteCastOperators.SHORT_1;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

public class BooleanCastOperators {

    public static <R> DataTypeCastOperator<Boolean, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case TINYINT:
                return (DataTypeCastOperator<Boolean, R>) (DataTypeCastOperator<Boolean, Byte>) b -> (Boolean.TRUE.equals(b) ? BYTE_1 : BYTE_0);
            case SMALLINT:
                return (DataTypeCastOperator<Boolean, R>) (DataTypeCastOperator<Boolean, Short>) b -> (Boolean.TRUE.equals(b) ? SHORT_1 : SHORT_0);
            case INT:
                return (DataTypeCastOperator<Boolean, R>) (DataTypeCastOperator<Boolean, Integer>) b -> (Boolean.TRUE.equals(b) ? INTEGER_1 : INTEGER_0);
            case BIGINT:
                return (DataTypeCastOperator<Boolean, R>) (DataTypeCastOperator<Boolean, Long>) b -> (Boolean.TRUE.equals(b) ? LONG_1 : LONG_0);
            case STRING:
                return (DataTypeCastOperator<Boolean, R>) (DataTypeCastOperator<Boolean, String>) b -> String.valueOf(b);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
