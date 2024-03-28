package se.kuseman.payloadbuilder.api.execution;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;

/**
 * Data type class for {@link Column.Type#DateTime}. Uses a long UTC epoch for internal storage/comparison.
 */
public class EpochDateTime implements Comparable<EpochDateTime>
{
    static final ZoneId UTC = ZoneId.of("UTC");
    private static final DateTimeFormatter ISO_DATE_OPTIONAL_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd['T'HH:mm[:ss][.nnnnnnnnn][[.SSS][.SS][.S]][X]]");
    private static final TemporalQuery<?>[] PARSE_QUERIES = new TemporalQuery[] { LocalDateTime::from, LocalDate::from };

    private final long epoch;

    /**
     * Lazy field that is created on demand and reused if wanted multiple times
     */
    private LocalDateTime dateTime;

    private EpochDateTime(long epoch)
    {
        this.epoch = epoch;
    }

    private EpochDateTime(LocalDateTime dateTime)
    {
        this.dateTime = requireNonNull(dateTime, "dateTime");
        this.epoch = dateTime.atZone(UTC)
                .toInstant()
                .toEpochMilli();
    }

    public long getEpoch()
    {
        return epoch;
    }

    /** Convert this datetime to offset with provided zone */
    public EpochDateTimeOffset toOffset(ZoneId zone)
    {
        return EpochDateTimeOffset.from(getLocalDateTime().atZone(zone));
    }

    /** Return this instance in {@link LocalDateTime} */
    public LocalDateTime getLocalDateTime()
    {
        if (dateTime == null)
        {
            dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(epoch), UTC);
        }
        return dateTime;
    }

    /** Get a part of this date time */
    public long getPart(ChronoField unit)
    {
        LocalDateTime t = getLocalDateTime();
        return t.getLong(unit);
    }

    /** Add a temporal unit to this datetime returning a new instance */
    public EpochDateTime add(long amount, TemporalUnit unit)
    {
        // Go concrete when duration is an estimate
        if (unit.isDurationEstimated())
        {
            return new EpochDateTime(getLocalDateTime().plus(amount, unit));
        }

        return new EpochDateTime(epoch + (amount * unit.getDuration()
                .toMillis()));
    }

    @Override
    public int compareTo(EpochDateTime o)
    {
        // NOTE! We don't include the zone here since all utcdatetimes
        // are a utc epoch. This to be able to compare two columns of different
        // timezones and still have a correct comparison regarding time
        return Long.compare(epoch, o.epoch);
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(epoch);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof EpochDateTime)
        {
            EpochDateTime that = (EpochDateTime) obj;
            return epoch == that.epoch;
        }
        return false;
    }

    @Override
    public String toString()
    {
        LocalDateTime instant = getLocalDateTime();
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(instant);
    }

    /** Creates a epoch date time with now instant */
    public static EpochDateTime now()
    {
        return new EpochDateTime(Instant.now()
                .toEpochMilli());
    }

    /** Creates a epoch date time from provided long unix epoch */
    public static EpochDateTime from(long epoch)
    {
        return new EpochDateTime(epoch);
    }

    /**
     * Create an epoch date time from provided string.
     */
    public static EpochDateTime from(String source)
    {
        String string = source.replace(' ', 'T');
        TemporalAccessor parsed;
        try
        {
            parsed = ISO_DATE_OPTIONAL_TIME_FORMATTER.parseBest(string, PARSE_QUERIES);
        }
        catch (DateTimeParseException e)
        {
            throw new IllegalArgumentException("Cannot cast '" + source + "' to " + Type.DateTime);
        }
        if (parsed instanceof LocalDateTime)
        {
            // ZoneOffset offset = ZoneOffset.systemDefault()
            return new EpochDateTime((LocalDateTime) parsed);
        }
        return new EpochDateTime(((LocalDate) parsed).atStartOfDay());
    }

    /** Try to convert provided object to a epoch date time */
    public static EpochDateTime from(Object object)
    {
        if (object instanceof EpochDateTime dt)
        {
            return dt;
        }
        else if (object instanceof EpochDateTimeOffset dto)
        {
            // Strip offset
            // Strip zone information
            ZonedDateTime zdt = dto.getZonedDateTime();
            return new EpochDateTime(zdt.toLocalDateTime());
        }
        else if (object instanceof Long l)
        {
            return new EpochDateTime(l);
        }
        else if (object instanceof String s)
        {
            return from(s);
        }
        else if (object instanceof UTF8String s)
        {
            return from(s.toString());
        }
        else if (object instanceof ZonedDateTime zdt)
        {
            // Strip zone information
            return new EpochDateTime(zdt.toLocalDateTime());
        }
        else if (object instanceof LocalDateTime ldt)
        {
            return new EpochDateTime(ldt);
        }
        else if (object instanceof LocalDate ld)
        {
            return new EpochDateTime(ld.atStartOfDay());
        }

        throw new IllegalArgumentException("Cannot cast '" + object
                                           + " ("
                                           + object.getClass()
                                                   .getSimpleName()
                                           + ")' to "
                                           + Type.DateTime);
    }
}
