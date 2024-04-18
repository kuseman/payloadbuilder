package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link AtTimeZoneExpression} */
public class AtTimeZoneExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test() throws Exception
    {
        AtTimeZoneExpression e;
        ValueVector actual;

        e = new AtTimeZoneExpression(new LiteralNullExpression(ResolvedType.of(Type.String)), new LiteralNullExpression(ResolvedType.of(Type.String)));
        assertEquals(ResolvedType.of(Type.DateTimeOffset), e.getType());
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), new Object[1]), actual);

        e = new AtTimeZoneExpression(new FunctionCallExpression("", SystemCatalog.get()
                .getScalarFunction("getdate"), null, asList()), new LiteralNullExpression(ResolvedType.of(Type.String)));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), new Object[1]), actual);

        e = new AtTimeZoneExpression(new FunctionCallExpression("", SystemCatalog.get()
                .getScalarFunction("getdate"), null, asList()), new LiteralStringExpression("dummy"));
        actual = e.eval(context);
        try
        {
            assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), "2000-10-10"), actual);
            fail("Should fail cause of invalid time zone");
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage(), ee.getMessage()
                    .contains("Time zone 'dummy' could not be found."));
        }

        e = new AtTimeZoneExpression(new LiteralStringExpression("hello"), new LiteralStringExpression("Z"));
        actual = e.eval(context);
        try
        {
            assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), "2000-10-10"), actual);
            fail("Should fail cause of invalid time zone");
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage(), ee.getMessage()
                    .contains("Cannot cast 'hello' to DateTime"));
        }

        e = new AtTimeZoneExpression(new LiteralStringExpression("2000-10-10"), new LiteralStringExpression("Z"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), "2000-10-10"), actual);

        e = new AtTimeZoneExpression(new LiteralStringExpression("2000-10-10T10:10:10"), new LiteralStringExpression("Z"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), ZonedDateTime.of(LocalDateTime.parse("2000-10-10T10:10:10"), ZoneId.of("UTC"))), actual);

        e = new AtTimeZoneExpression(new LiteralStringExpression("2000-10-10T12:10:10+02:00"), new LiteralStringExpression("Z"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), ZonedDateTime.parse("2000-10-10T10:10:10Z")), actual);

        // Test zoning a DateTime
        // 1. We cast '2000-10-10T12:10:10' to datetime => we get a LocalDateTime = 2000-10-10T12:10:10
        // 2. We set the zone to "Europe/Berlin" yielding 2000-10-10T12:10:10+02:00 (we don't change any date/time here only zone)
        e = new AtTimeZoneExpression(new CastExpression(new LiteralStringExpression("2000-10-10T12:10:10"), ResolvedType.of(Type.DateTime)), new LiteralStringExpression("Europe/Berlin"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), ZonedDateTime.parse("2000-10-10T12:10:10+02:00")), actual);

        // Test zoning a casted DateTimeOffset
        // 1. We cast '2000-10-10T12:10:10' to datetime => we get a LocalDateTime = 2000-10-10T12:10:10
        // 2. We cast that to UTC offset => we get a ZonedDateTime = 2000-10-10T12:10:10+00:00
        // 3. We set the zone to "Europe/Berlin" and now zone rules kicks in and we get "2000-10-10T14:10:10+02:00"
        e = new AtTimeZoneExpression(new CastExpression(new CastExpression(new LiteralStringExpression("2000-10-10T12:10:10"), ResolvedType.of(Type.DateTime)), ResolvedType.of(Type.DateTimeOffset)),
                new LiteralStringExpression("Europe/Berlin"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), ZonedDateTime.parse("2000-10-10T14:10:10+02:00")), actual);
    }
}
