package se.kuseman.payloadbuilder.api.execution;

import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.execution.EpochDateTime.UTC;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalUnit;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/**
 * Data type class for {@link Column.Type#DateTimeOffset}. Extends {@link EpochDateTime} with a timezone
 */
public class EpochDateTimeOffset implements Comparable<EpochDateTimeOffset>, ValueVector
{
    private static final DateTimeFormatter ISO_DATE_OPTIONAL_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd['T'HH:mm[:ss][.nnnnnnnnn][.[SSS][SS][S]][xxx][X]]");

    private final long epoch;
    /** Which zone this date time is at. Used when retrieving */
    private final ZoneId zoneId;
    private ZonedDateTime dateTime;

    private EpochDateTimeOffset(long epoch, ZoneId zoneId)
    {
        this.epoch = epoch;
        this.zoneId = requireNonNull(zoneId, "zoneId");
    }

    private EpochDateTimeOffset(ZonedDateTime dateTime)
    {
        this.dateTime = requireNonNull(dateTime, "dateTime");
        this.epoch = dateTime.toInstant()
                .toEpochMilli();
        this.zoneId = dateTime.getZone();
    }

    // ValueVector

    @Override
    public int size()
    {
        return 1;
    }

    @Override
    public ResolvedType type()
    {
        return ResolvedType.of(Type.DateTimeOffset);
    }

    @Override
    public boolean isNull(int row)
    {
        return false;
    }

    @Override
    public EpochDateTimeOffset getDateTimeOffset(int row)
    {
        return this;
    }

    // End ValueVector

    public long getEpoch()
    {
        return epoch;
    }

    /** Return this instance in {@link ZonedDateTime} */
    public ZonedDateTime getZonedDateTime()
    {
        if (dateTime == null)
        {
            dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epoch), zoneId);
        }
        return dateTime;
    }

    /** Get a part of this date time */
    public long getPart(ChronoField unit)
    {
        ZonedDateTime t = getZonedDateTime();
        return t.getLong(unit);
    }

    /** Add a temporal unit to this datetime returning a new instance */
    public EpochDateTimeOffset add(long amount, TemporalUnit unit)
    {
        // Go concrete when duration is an estimate
        if (unit.isDurationEstimated())
        {
            return new EpochDateTimeOffset(getZonedDateTime().plus(amount, unit));
        }

        return new EpochDateTimeOffset(epoch + (amount * unit.getDuration()
                .toMillis()), zoneId);
    }

    /** Switch time zone */
    public EpochDateTimeOffset atZone(ZoneId zone)
    {
        return new EpochDateTimeOffset(epoch, zone);
    }

    @Override
    public int compareTo(EpochDateTimeOffset o)
    {
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
        else if (obj instanceof EpochDateTimeOffset)
        {
            EpochDateTimeOffset that = (EpochDateTimeOffset) obj;
            return epoch == that.epoch;
        }
        return false;
    }

    @Override
    public String toString()
    {
        ZonedDateTime dateTime = getZonedDateTime();
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(dateTime);
    }

    /** Creates a epoch date time with now instant in UTC */
    public static EpochDateTimeOffset now()
    {
        return new EpochDateTimeOffset(Instant.now()
                .toEpochMilli(), UTC);
    }

    /** Creates a epoch date time from provided long unix epoch in UTC */
    public static EpochDateTimeOffset from(long epoch)
    {
        return new EpochDateTimeOffset(epoch, UTC);
    }

    /**
     * Create an epoch date time from provided string.
     */
    public static EpochDateTimeOffset from(String source)
    {
        String string = source.replace(' ', 'T');
        TemporalAccessor parsed;
        try
        {
            parsed = ISO_DATE_OPTIONAL_TIME_FORMATTER.parseBest(string, ZonedDateTime::from, LocalDateTime::from, LocalDate::from);
        }
        catch (DateTimeParseException e)
        {
            throw new IllegalArgumentException("Cannot cast '" + source + "' to " + Type.DateTimeOffset);
        }
        if (parsed instanceof ZonedDateTime)
        {
            ZonedDateTime zdt = (ZonedDateTime) parsed;
            return new EpochDateTimeOffset(zdt);
        }
        if (parsed instanceof LocalDateTime)
        {
            // Convert the date time to UTC offset
            return new EpochDateTimeOffset(((LocalDateTime) parsed).atZone(UTC));
        }
        else if (parsed instanceof LocalDate)
        {
            return new EpochDateTimeOffset(((LocalDate) parsed).atStartOfDay()
                    .atZone(UTC));
        }

        throw new IllegalArgumentException("Cannot cast '" + source + "' to " + Type.DateTimeOffset);
    }

    /** Reflective convert. Try to convert provided object to a epoch date time */
    public static EpochDateTimeOffset from(Object object)
    {
        if (object instanceof EpochDateTimeOffset dto)
        {
            return dto;
        }
        else if (object instanceof EpochDateTime dt)
        {
            return new EpochDateTimeOffset(dt.getEpoch(), UTC);
        }
        else if (object instanceof Long l)
        {
            return new EpochDateTimeOffset(l, UTC);
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
            return new EpochDateTimeOffset(zdt);
        }
        else if (object instanceof LocalDateTime ldt)
        {
            return new EpochDateTimeOffset(ldt.atZone(UTC));
        }
        else if (object instanceof LocalDate ld)
        {
            return new EpochDateTimeOffset(ld.atStartOfDay()
                    .atZone(UTC));
        }

        throw new IllegalArgumentException("Cannot cast '" + object
                                           + " ("
                                           + object.getClass()
                                                   .getSimpleName()
                                           + ")' to "
                                           + Type.DateTimeOffset);
    }
}
