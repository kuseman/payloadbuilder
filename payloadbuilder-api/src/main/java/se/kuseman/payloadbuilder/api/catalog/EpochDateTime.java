package se.kuseman.payloadbuilder.api.catalog;

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
import java.time.temporal.TemporalUnit;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;

/**
 * Wrapper class for a date time. Uses a long UTC epoch for internal storage/comparison.
 */
public class EpochDateTime implements Comparable<EpochDateTime>
{
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final DateTimeFormatter ISO_DATE_OPTIONAL_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd['T'HH:mm[:ss][.nnnnnnnnn][.SSS][xxx][X]]");

    /** The epoch is always stored as epoch millis to be able to compare different zones */
    private final long epoch;
    /** Which zone this date time is at. Used when retrieving */
    private final ZoneId zone;
    /** Flag that indicates if this date has a explicit zone set by user. */
    private final boolean explicitZone;

    /**
     * Lazy field that is created on demand and reused if wanted multiple times. Ie. fetching many date parts from a column then we only create one instance
     */
    private ZonedDateTime dateTime;

    private EpochDateTime(long epoch, boolean explicitZone)
    {
        this(epoch, UTC, explicitZone);
    }

    private EpochDateTime(long epoch, ZoneId zone, boolean explicitZone)
    {
        this.epoch = epoch;
        this.zone = requireNonNull(zone, "zone");
        this.explicitZone = explicitZone;
    }

    private EpochDateTime(ZonedDateTime dateTime, boolean explicitZone)
    {
        this.dateTime = requireNonNull(dateTime, "dateTime");
        this.epoch = dateTime.toInstant()
                .toEpochMilli();
        this.zone = dateTime.getZone();
        this.explicitZone = explicitZone;
    }

    public long getEpoch()
    {
        return epoch;
    }

    public boolean isExplicitZone()
    {
        return explicitZone;
    }

    /** Return this instance in {@link ZonedDateTime} */
    public ZonedDateTime toZonedDateTime()
    {
        if (dateTime == null)
        {
            dateTime = Instant.ofEpochMilli(epoch)
                    .atZone(zone);
        }
        return dateTime;
    }

    /** Get a part of this date time */
    public int getPart(ChronoField unit)
    {
        ZonedDateTime zd = toZonedDateTime();
        return zd.get(unit);
    }

    /** Add a temporal unit to this datetime returning a new instance */
    public EpochDateTime add(long amount, TemporalUnit unit)
    {
        // Go concrete when duration is an estimate
        if (unit.isDurationEstimated())
        {
            return new EpochDateTime(toZonedDateTime().plus(amount, unit), explicitZone);
        }

        return new EpochDateTime(epoch + (amount * unit.getDuration()
                .toMillis()), zone, explicitZone);
    }

    /** Zone this date time */
    public EpochDateTime atZone(ZoneId zone)
    {
        return new EpochDateTime(epoch, zone, true);
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
        ZonedDateTime dateTime = toZonedDateTime();
        // If the zone is in the system default and is not explicitly set by user the we don't return the zone
        if (!explicitZone
                && ZoneId.systemDefault()
                        .equals(dateTime.getZone()))
        {
            return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(dateTime);
        }
        else
        {
            // else return zone with +03:00 format
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(dateTime);
        }
    }

    /** Creates a epoch date time with now instant */
    public static EpochDateTime now()
    {
        return new EpochDateTime(Instant.now()
                .toEpochMilli(), false);
    }

    /** Creates a epoch date time from provided long unix epoch */
    public static EpochDateTime from(long epoch)
    {
        return new EpochDateTime(epoch, false);
    }

    /**
     * Create an epoch date time from provided string. The returned date is converted to system default zone if zoned Throws exception if a date cannot be constructed
     */
    public static EpochDateTime from(String source)
    {
        String string = source.replace(' ', 'T');
        TemporalAccessor parsed;
        try
        {
            parsed = ISO_DATE_OPTIONAL_TIME_FORMATTER.parseBest(string, ZonedDateTime::from, LocalDateTime::from, LocalDate::from);
        }
        catch (DateTimeParseException e)
        {
            throw new IllegalArgumentException("Cannot cast '" + source + "' to " + Type.DateTime);
        }
        if (parsed instanceof ZonedDateTime)
        {
            ZonedDateTime zdt = (ZonedDateTime) parsed;
            return new EpochDateTime(zdt.withZoneSameInstant(ZoneId.systemDefault()), false);
        }
        if (parsed instanceof LocalDateTime)
        {
            return new EpochDateTime(((LocalDateTime) parsed).atZone(ZoneId.systemDefault()), false);
        }
        else if (parsed instanceof LocalDate)
        {
            return new EpochDateTime(((LocalDate) parsed).atStartOfDay()
                    .atZone(ZoneId.systemDefault()), false);
        }

        throw new IllegalArgumentException("Cannot cast '" + source + "' to " + Type.DateTime);
    }

    /** Try to convert provided object to a epoch date time */
    public static EpochDateTime from(Object object)
    {
        if (object instanceof EpochDateTime)
        {
            return (EpochDateTime) object;
        }
        else if (object instanceof Long)
        {
            return new EpochDateTime((Long) object, false);
        }
        else if (object instanceof String)
        {
            return from((String) object);
        }
        else if (object instanceof ZonedDateTime)
        {
            return new EpochDateTime((ZonedDateTime) object, false);
        }
        else if (object instanceof LocalDateTime)
        {
            return new EpochDateTime(((LocalDateTime) object).atZone(ZoneId.systemDefault()), false);
        }
        else if (object instanceof LocalDate)
        {
            return from(((LocalDate) object).atStartOfDay());
        }

        throw new IllegalArgumentException("Cannot cast '" + object
                                           + " ("
                                           + object.getClass()
                                                   .getSimpleName()
                                           + ")' to "
                                           + Type.DateTime);
    }
}
