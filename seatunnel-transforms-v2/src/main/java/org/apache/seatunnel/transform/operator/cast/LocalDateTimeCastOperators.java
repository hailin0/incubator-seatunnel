package org.apache.seatunnel.transform.operator.cast;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class LocalDateTimeCastOperators {

    public static <R> DataTypeCastOperator<LocalDateTime, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BIGINT:
                return (DataTypeCastOperator<LocalDateTime, R>) (DataTypeCastOperator<LocalDateTime, Long>) s -> Timestamp.valueOf(s).getTime();
            case STRING:
                return (DataTypeCastOperator<LocalDateTime, R>) (DataTypeCastOperator<LocalDateTime, String>) s -> s.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            case DATE:
                return (DataTypeCastOperator<LocalDateTime, R>) (DataTypeCastOperator<LocalDateTime, LocalDate>) s -> s.toLocalDate();
            case TIME:
                return (DataTypeCastOperator<LocalDateTime, R>) (DataTypeCastOperator<LocalDateTime, LocalTime>) s -> s.toLocalTime();
            default:
                throw new UnsupportedOperationException();
        }
    }
}
