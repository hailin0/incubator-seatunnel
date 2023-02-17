package org.apache.seatunnel.transform.operator.cast;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class LocalTimeCastOperators {

    public static <R> DataTypeCastOperator<LocalTime, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case INT:
                return (DataTypeCastOperator<LocalTime, R>) (DataTypeCastOperator<LocalTime, Integer>) s -> s.toSecondOfDay();
            case BIGINT:
                return (DataTypeCastOperator<LocalTime, R>) (DataTypeCastOperator<LocalTime, Long>) s -> s.toNanoOfDay();
            case STRING:
                return (DataTypeCastOperator<LocalTime, R>) (DataTypeCastOperator<LocalTime, String>) s -> s.format(DateTimeFormatter.ISO_LOCAL_TIME);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
