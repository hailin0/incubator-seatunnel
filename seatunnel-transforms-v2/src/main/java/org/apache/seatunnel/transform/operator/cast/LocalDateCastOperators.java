package org.apache.seatunnel.transform.operator.cast;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class LocalDateCastOperators {

    public static <R> DataTypeCastOperator<LocalDate, R> create(SeaTunnelDataType<R> outputType) {
        switch (outputType.getSqlType()) {
            case BIGINT:
                return (DataTypeCastOperator<LocalDate, R>) (DataTypeCastOperator<LocalDate, Long>) s -> s.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
            case STRING:
                return (DataTypeCastOperator<LocalDate, R>) (DataTypeCastOperator<LocalDate, String>) s -> s.format(DateTimeFormatter.ISO_LOCAL_DATE);
            case TIMESTAMP:
                return (DataTypeCastOperator<LocalDate, R>) (DataTypeCastOperator<LocalDate, LocalDateTime>) s -> s.atStartOfDay(ZoneId.systemDefault()).toLocalDateTime();
            default:
                throw new UnsupportedOperationException();
        }
    }
}
