package org.apache.seatunnel.transform.operator.cast;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;

public class ArrayCastOperators {

    public static <R> DataTypeCastOperator<Object[], R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case STRING:
                return (DataTypeCastOperator<Object[], R>) (DataTypeCastOperator<Object[], String>) array -> {
                    StringJoiner joiner = new StringJoiner(",", "[", "]");
                    for (Object item : array) {
                        joiner.add(item.toString());
                    }
                    return joiner.toString();
                };
            default:
                throw new UnsupportedOperationException();
        }
    }
}
