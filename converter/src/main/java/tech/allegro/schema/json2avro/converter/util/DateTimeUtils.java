package tech.allegro.schema.json2avro.converter.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

public class DateTimeUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeUtils.class);

    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("[yyyy][yy]['-']['/']['.'][' '][MMM][MM][M]['-']['/']['.'][' '][dd][d]" +
                    "[[' ']['T']HH:mm[':'ss[.][SSSSSS][SSSSS][SSSS][SSS][' '][z][zzz][Z][O][x][XXX][XX][X]]]");
    private static final DateTimeFormatter timeFormatter =
            DateTimeFormatter.ofPattern("HH:mm[':'ss[.][SSSSSS][SSSSS][SSSS][SSS]]");

    public static Long getEpochMillis(String dateTime) {
        Instant instant = null;
        if (dateTime.matches("-?\\d+")) {
            return Long.valueOf(dateTime);
        }
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(dateTime, formatter);
            instant = zdt.toInstant();
        } catch (DateTimeParseException e) {
            try {
                LocalDateTime dt = LocalDateTime.parse(dateTime, formatter);
                instant = dt.toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException ex) {
                LOGGER.error("Failed to parse date-time :" + dateTime);
            }
        }
        return Objects.requireNonNull(instant).toEpochMilli();
    }

    public static Integer getEpochDay(String dateTime) {
        Integer epochDay = null;
        try {
            LocalDate date = LocalDate.parse(dateTime, formatter);
            epochDay = (int) date.toEpochDay();
        } catch (DateTimeParseException e) {
            LOGGER.error("Failed to parse date :" + dateTime);
        }
        return Objects.requireNonNull(epochDay);
    }

    public static Long getMicroSeconds(String dateTime) {
        Long secondOfDay = null;
        if (dateTime.matches("-?\\d+")) {
            return Long.valueOf(dateTime);
        }
        try {
            LocalTime time = LocalTime.parse(dateTime, timeFormatter);
            secondOfDay = time.toNanoOfDay();
        } catch (DateTimeParseException e) {
            try {
                LocalTime time = LocalTime.parse(dateTime,formatter);
                secondOfDay = time.toNanoOfDay();
            }catch (DateTimeParseException ex){
                LOGGER.error("Failed to parse time :" + dateTime);
            }
        }
        if (secondOfDay==null){
            throw new RuntimeException("Failed to parse time :" + dateTime);
        }
        return secondOfDay / 1000;
    }
}
