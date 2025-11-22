package se.kuseman.payloadbuilder.core.physicalplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Test of {@link EpochDateTimeOffset} */
class EpochDateTimeOffsetTest
{
    @Test
    void test()
    {
        EpochDateTimeOffset d = EpochDateTimeOffset.now();
        assertFalse(d.hasNulls());
        assertFalse(d.equals(null));
        assertTrue(d.equals(d));
        assertFalse(d.equals(EpochDateTimeOffset.from("2010-10-10T00:10:00.00Z")));

        assertSame(d, EpochDateTimeOffset.from(d));

        assertEquals(EpochDateTimeOffset.from("2010-10-10T00:10:00.00Z"), EpochDateTimeOffset.from((Object) EpochDateTimeOffset.from("2010-10-10T00:10:00.00Z")
                .getEpoch()));
        assertEquals(EpochDateTimeOffset.from("2010-10-10T00:10:00.00Z"), EpochDateTimeOffset.from((Object) "2010-10-10T00:10:00.00Z"));

        // From EpochDateTime
        assertEquals(EpochDateTimeOffset.from("2010-10-10T00:10:00.00Z"), EpochDateTimeOffset.from(EpochDateTime.from("2010-10-10T00:10:00.00Z")));

        Object o = EpochDateTimeOffset.from("2010-10-10T00:10:00.00Z")
                .getZonedDateTime();

        assertEquals(EpochDateTimeOffset.from("2010-10-10T00:10:00.00Z"), EpochDateTimeOffset.from(o));

        assertEquals(EpochDateTimeOffset.from("2010-10-10T00:10:00.00Z"), EpochDateTimeOffset.from(OffsetDateTime.parse("2010-10-10T00:10:00.00Z")));

        o = EpochDateTimeOffset.from("2010-10-10T00:10:00.00Z")
                .getZonedDateTime()
                .toLocalDate();

        assertEquals(EpochDateTime.from("2010-10-10"), EpochDateTime.from(o));

        try
        {
            EpochDateTimeOffset.from(10F);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage()
                    .contains("Cannot cast '10.0 (Float)' to DateTimeOffset"), e.getMessage());
        }

        assertEquals(EpochDateTimeOffset.from("2010-10-10T00:00:00Z"), EpochDateTimeOffset.from(LocalDate.parse("2010-10-10")));
    }

    @Test
    void test_add()
    {
        long longNow = Instant.parse("2010-10-10T00:10:00.00Z")
                .toEpochMilli();
        EpochDateTimeOffset now = EpochDateTimeOffset.from(longNow);

        assertEquals("2010-10-10T00:10:00Z", now.toString());

        assertEquals("2010-10-10T02:10:00+02:00", now.atZone(ZoneId.of("Europe/Berlin"))
                .toString());

        EpochDateTimeOffset actual = now.add(10, ChronoUnit.YEARS);

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
    void test_from_string()
    {
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2010, 10, 10, 0, 0, 0)), EpochDateTimeOffset.from("2010-10-10"));
        // Space separator
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2010, 10, 10, 10, 10, 10)), EpochDateTimeOffset.from("2010-10-10 10:10:10"));
        // ISO T separator
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2020, 10, 10, 10, 20, 10)), EpochDateTimeOffset.from("2020-10-10T10:20:10"));
        // UTC marker
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2010, 10, 10, 00, 10, 10)), EpochDateTimeOffset.from("2010-10-10T00:10:10Z"));
        // UTC marker with nanos
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTimeOffset.from("2023-07-04T11:55:07.600000000Z"));
        // UTC marker with second fraction (3 digit)
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTimeOffset.from("2023-07-04T11:55:07.600Z"));
        // UTC marker with second fraction (2 digit)
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTimeOffset.from("2023-07-04T11:55:07.60Z"));
        // UTC marker with second fraction (1 digit)
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTimeOffset.from("2023-07-04T11:55:07.6Z"));

        // With offset
        assertEquals(EpochDateTimeOffset.from(ZonedDateTime.of(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000), ZoneOffset.of("+03:00"))),
                EpochDateTimeOffset.from("2023-07-04T11:55:07.6+03:00"));

        try
        {
            EpochDateTimeOffset.from("ohhh no");
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage()
                    .contains("Cannot cast 'ohhh no' to DateTime"), e.getMessage());
        }
    }

    @Test
    void test_from_uft8_string()
    {
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2010, 10, 10, 0, 0, 0)), EpochDateTimeOffset.from(UTF8String.from("2010-10-10")));
        // Space separator
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2010, 10, 10, 10, 10, 10)), EpochDateTimeOffset.from(UTF8String.from("2010-10-10 10:10:10")));
        // ISO T separator
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2020, 10, 10, 10, 20, 10)), EpochDateTimeOffset.from(UTF8String.from("2020-10-10T10:20:10")));
        // UTC marker
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2010, 10, 10, 00, 10, 10)), EpochDateTimeOffset.from(UTF8String.from("2010-10-10T00:10:10Z")));
        // UTC marker with nanos
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTimeOffset.from(UTF8String.from("2023-07-04T11:55:07.600000000Z")));
        // UTC marker with second fraction (3 digit)
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTimeOffset.from(UTF8String.from("2023-07-04T11:55:07.600Z")));
        // UTC marker with second fraction (2 digit)
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTimeOffset.from(UTF8String.from("2023-07-04T11:55:07.60Z")));
        // UTC marker with second fraction (1 digit)
        assertEquals(EpochDateTimeOffset.from(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000)), EpochDateTimeOffset.from(UTF8String.from("2023-07-04T11:55:07.6Z")));

        // With offset
        assertEquals(EpochDateTimeOffset.from(ZonedDateTime.of(LocalDateTime.of(2023, 07, 04, 11, 55, 07, 600000000), ZoneOffset.of("+03:00"))),
                EpochDateTimeOffset.from(UTF8String.from("2023-07-04T11:55:07.6+03:00")));

        try
        {
            EpochDateTimeOffset.from(UTF8String.from("ohhh no"));
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage()
                    .contains("Cannot cast 'ohhh no' to DateTime"), e.getMessage());
        }
    }
    //
    // @Ignore
    // @Test
    // void test1()
    // {
    // long n = System.currentTimeMillis();
    //
    // // ZoneId zone = ZoneId.of("UTC");
    // EpochDateTime d = EpochDateTime.from(n);
    // // ZonedDateTime zd = Instant.ofEpochMilli(n)
    // // .atZone(zone);
    //
    // for (int i = 1; i < 100_000_000; i++)
    // {
    // EpochDateTime now = EpochDateTime.from(n + i);
    // assertEquals(1, now.compareTo(d));
    //
    // // int hour = now.getPart(ChronoField.HOUR_OF_DAY);
    // //
    // // assertTrue(hour > 0);
    //
    // // System.out.println(now.getPart(ChronoField.DAY_OF_WEEK));
    //
    // // System.out.println(now.toLocalDateTime() + " " + now.toZonedDateTime());
    // // ZonedDateTime d1 = Instant.ofEpochMilli(n + 1)
    // // .atZone(zone);
    // //
    // // assertTrue(d1.compareTo(zd) >= 1);
    //
    // }
    //
    // // ZonedDateTime d = ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC"));
    // //
    // // long utcMilli = d.toInstant()
    // // .toEpochMilli();
    // //
    // // System.out.println(d.plus(10, ChronoField.DAY_OF_YEAR.getBaseUnit()));
    // //
    // // System.out.println(Instant.ofEpochMilli(utcMilli + (10 * 24 * 60 * 60 * 1000))
    // // .atZone(ZoneId.of("UTC")));
    //
    // }
}
