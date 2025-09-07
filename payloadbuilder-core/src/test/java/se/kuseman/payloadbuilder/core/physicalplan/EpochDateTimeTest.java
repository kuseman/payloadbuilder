package se.kuseman.payloadbuilder.core.physicalplan;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Test of {@link EpochDateTime} */
public class EpochDateTimeTest extends Assert
{
    @Test
    public void test()
    {
        EpochDateTime d = EpochDateTime.now();
        assertFalse(d.equals(null));
        assertTrue(d.equals(d));
        assertFalse(d.equals(EpochDateTime.from("2010-10-10T00:10:00.00Z")));

        assertSame(d, EpochDateTime.from(d));

        assertEquals(EpochDateTime.from("2010-10-10T00:10:00.00Z"), EpochDateTime.from((Object) EpochDateTime.from("2010-10-10T00:10:00.00Z")
                .getEpoch()));
        assertEquals(EpochDateTime.from("2010-10-10T00:10:00.00Z"), EpochDateTime.from((Object) "2010-10-10T00:10:00.00Z"));

        // Test that offsets are stripped from zoned date time
        assertEquals(EpochDateTime.from("2010-10-10T00:10:00.00Z"), EpochDateTime.from(ZonedDateTime.parse("2010-10-10T00:10:00.00+03:00")));
        assertEquals(EpochDateTime.from("2010-10-10T00:10:00.00Z"), EpochDateTime.from(EpochDateTimeOffset.from(ZonedDateTime.parse("2010-10-10T00:10:00.00+03:00"))));
        assertEquals(EpochDateTime.from("2010-10-10T00:10:00.00Z"), EpochDateTime.from(OffsetDateTime.parse("2010-10-10T00:10:00.00+03:00")));

        Object o = EpochDateTime.from("2010-10-10T00:10:00.00Z")
                .getLocalDateTime();

        assertEquals(EpochDateTime.from("2010-10-10T00:10:00.00Z"), EpochDateTime.from(o));

        o = EpochDateTime.from("2010-10-10T00:10:00.00Z")
                .getLocalDateTime()
                .toLocalDate();

        assertEquals(EpochDateTime.from("2010-10-10"), EpochDateTime.from(o));

        try
        {
            EpochDateTime.from(10F);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast '10.0 (Float)' to DateTime"));
        }
    }

    @Test
    public void test_add()
    {
        long longNow = Instant.parse("2010-10-10T00:10:00.00Z")
                .toEpochMilli();
        EpochDateTime now = EpochDateTime.from(longNow);

        assertEquals("2010-10-10T00:10:00", now.toString());

        assertEquals("2010-10-10T00:10:00+02:00", now.toOffset(ZoneId.of("Europe/Berlin"))
                .toString());

        EpochDateTime actual = now.add(10, ChronoUnit.YEARS);

        assertEquals(2020, actual.getPart(ChronoField.YEAR));
        assertEquals(10, actual.getPart(ChronoField.MONTH_OF_YEAR));
        assertEquals(10, actual.getPart(ChronoField.DAY_OF_MONTH));
        assertEquals(00, actual.getPart(ChronoField.HOUR_OF_DAY));
        assertEquals(10, actual.getPart(ChronoField.MINUTE_OF_HOUR));

        ZonedDateTime expected = ZonedDateTime.ofInstant(Instant.ofEpochMilli(longNow), ZoneId.of("UTC"))
                .plus(10, ChronoUnit.YEARS);

        assertEquals(expected.toInstant()
                .toEpochMilli(), actual.getEpoch());

        actual = now.add(93, ChronoUnit.HOURS);
        assertEquals(2010, actual.getPart(ChronoField.YEAR));
        assertEquals(10, actual.getPart(ChronoField.MONTH_OF_YEAR));
        assertEquals(13, actual.getPart(ChronoField.DAY_OF_MONTH));
        assertEquals(21, actual.getPart(ChronoField.HOUR_OF_DAY));
        assertEquals(10, actual.getPart(ChronoField.MINUTE_OF_HOUR));

        expected = ZonedDateTime.ofInstant(Instant.ofEpochMilli(longNow), ZoneId.of("UTC"))
                .plus(93, ChronoUnit.HOURS);

        assertEquals(expected.toInstant()
                .toEpochMilli(), actual.getEpoch());

        // Assert compare to
        assertTrue(actual.compareTo(actual.add(1, ChronoUnit.DAYS)) < 0);
        assertTrue(actual.compareTo(actual.add(-1, ChronoUnit.DAYS)) > 0);
        assertTrue(actual.compareTo(actual.add(0, ChronoUnit.DAYS)) == 0);
    }

    @Test
    public void test_from_string()
    {
        assertEquals(EpochDateTime.from(LocalDateTime.of(2010, 10, 10, 0, 0, 0)), EpochDateTime.from("2010-10-10"));
        // Space separator
        assertEquals(EpochDateTime.from(LocalDateTime.of(2010, 10, 10, 10, 10, 10)), EpochDateTime.from("2010-10-10 10:10:10"));
        // ISO T separator
        assertEquals(EpochDateTime.from(LocalDateTime.of(2020, 10, 10, 10, 20, 10)), EpochDateTime.from("2020-10-10T10:20:10"));
        // UTC marker
        assertEquals(EpochDateTime.from(LocalDateTime.of(2010, 10, 10, 00, 10, 10)), EpochDateTime.from("2010-10-10T00:10:10Z"));
        // UTC marker with nanos
        assertEquals(EpochDateTime.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTime.from("2023-07-04T11:55:07.600000000Z"));
        // UTC marker with second fraction (3 digit)
        assertEquals(EpochDateTime.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTime.from("2023-07-04T11:55:07.600Z"));
        // UTC marker with second fraction (2 digit)
        assertEquals(EpochDateTime.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTime.from("2023-07-04T11:55:07.60Z"));
        // UTC marker with second fraction (1 digit)
        assertEquals(EpochDateTime.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTime.from("2023-07-04T11:55:07.6Z"));

        try
        {
            EpochDateTime.from("ohhh no");
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 'ohhh no' to DateTime"));
        }
    }

    @Test
    public void test_from_uf8_string()
    {
        assertEquals(EpochDateTime.from(LocalDateTime.of(2010, 10, 10, 0, 0, 0)), EpochDateTime.from(UTF8String.from("2010-10-10")));
        // Space separator
        assertEquals(EpochDateTime.from(LocalDateTime.of(2010, 10, 10, 10, 10, 10)), EpochDateTime.from(UTF8String.from("2010-10-10 10:10:10")));
        // ISO T separator
        assertEquals(EpochDateTime.from(LocalDateTime.of(2020, 10, 10, 10, 20, 10)), EpochDateTime.from(UTF8String.from("2020-10-10T10:20:10")));
        // UTC marker
        assertEquals(EpochDateTime.from(LocalDateTime.of(2010, 10, 10, 00, 10, 10)), EpochDateTime.from(UTF8String.from("2010-10-10T00:10:10Z")));
        // UTC marker with nanos
        assertEquals(EpochDateTime.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTime.from(UTF8String.from("2023-07-04T11:55:07.600000000Z")));
        // UTC marker with second fraction (3 digit)
        assertEquals(EpochDateTime.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTime.from(UTF8String.from("2023-07-04T11:55:07.600Z")));
        // UTC marker with second fraction (2 digit)
        assertEquals(EpochDateTime.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTime.from(UTF8String.from("2023-07-04T11:55:07.60Z")));
        // UTC marker with second fraction (1 digit)
        assertEquals(EpochDateTime.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTime.from(UTF8String.from("2023-07-04T11:55:07.6Z")));

        try
        {
            EpochDateTime.from(UTF8String.from("ohhh no"));
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 'ohhh no' to DateTime"));
        }
    }

    @Ignore
    @Test
    public void test1()
    {
        long n = System.currentTimeMillis();

        // ZoneId zone = ZoneId.of("UTC");
        EpochDateTime d = EpochDateTime.from(n);
        // ZonedDateTime zd = Instant.ofEpochMilli(n)
        // .atZone(zone);

        for (int i = 1; i < 100_000_000; i++)
        {
            EpochDateTime now = EpochDateTime.from(n + i);
            assertEquals(1, now.compareTo(d));

            // int hour = now.getPart(ChronoField.HOUR_OF_DAY);
            //
            // assertTrue(hour > 0);

            // System.out.println(now.getPart(ChronoField.DAY_OF_WEEK));

            // System.out.println(now.toLocalDateTime() + " " + now.toZonedDateTime());
            // ZonedDateTime d1 = Instant.ofEpochMilli(n + 1)
            // .atZone(zone);
            //
            // assertTrue(d1.compareTo(zd) >= 1);

        }

        // ZonedDateTime d = ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC"));
        //
        // long utcMilli = d.toInstant()
        // .toEpochMilli();
        //
        // System.out.println(d.plus(10, ChronoField.DAY_OF_YEAR.getBaseUnit()));
        //
        // System.out.println(Instant.ofEpochMilli(utcMilli + (10 * 24 * 60 * 60 * 1000))
        // .atZone(ZoneId.of("UTC")));

    }
}
