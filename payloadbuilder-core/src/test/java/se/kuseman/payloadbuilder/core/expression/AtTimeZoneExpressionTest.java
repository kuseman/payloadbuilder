package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.nvv;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
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

        e = new AtTimeZoneExpression(new LiteralNullExpression(Type.String), new LiteralNullExpression(Type.String));
        actual = e.eval(context);
        assertVectorsEquals(nvv(ResolvedType.of(Type.DateTime), new Object[1]), actual);

        e = new AtTimeZoneExpression(new FunctionCallExpression("", SystemCatalog.get()
                .getScalarFunction("getdate"), null, asList()), new LiteralNullExpression(Type.String));
        actual = e.eval(context);
        assertVectorsEquals(nvv(ResolvedType.of(Type.DateTime), new Object[1]), actual);

        e = new AtTimeZoneExpression(new FunctionCallExpression("", SystemCatalog.get()
                .getScalarFunction("getdate"), null, asList()), new LiteralStringExpression("dummy"));
        actual = e.eval(context);
        try
        {
            assertVectorsEquals(vv(ResolvedType.of(Type.DateTime), "2000-10-10"), actual);
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
            assertVectorsEquals(vv(ResolvedType.of(Type.DateTime), "2000-10-10"), actual);
            fail("Should fail cause of invalid time zone");
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage(), ee.getMessage()
                    .contains("Cannot cast 'hello' to DateTime"));
        }

        e = new AtTimeZoneExpression(new LiteralStringExpression("2000-10-10"), new LiteralStringExpression("Z"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTime), "2000-10-10"), actual);

        e = new AtTimeZoneExpression(new LiteralStringExpression("2000-10-10T10:10:10"), new LiteralStringExpression("Z"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTime), ZonedDateTime.of(LocalDateTime.parse("2000-10-10T10:10:10"), ZoneId.systemDefault())
                .withZoneSameInstant(ZoneId.of("Z"))), actual);

        e = new AtTimeZoneExpression(new LiteralStringExpression("2000-10-10T12:10:10+02:00"), new LiteralStringExpression("Z"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTime), ZonedDateTime.parse("2000-10-10T10:10:10Z")), actual);

        //
        // Map<String, Object> values = new HashMap<>();
        // values.put("a", LocalDate.parse("2000-10-10"));
        // values.put("b", LocalDateTime.parse("2000-10-10T10:10:10"));
        // values.put("c", ZonedDateTime.parse("2000-10-10T12:10:10+02:00"));
        //
        // Object expected = ZonedDateTime.of(LocalDate.parse("2000-10-10"), LocalTime.parse("00:00:00"), ZoneId.systemDefault())
        // .withZoneSameInstant(ZoneId.of("Z"));
        //
        // assertExpression(expected, values, "attimezone(a, 'Z')");
        //
        // expected = ZonedDateTime.of(LocalDateTime.parse("2000-10-10T10:10:10"), ZoneId.systemDefault())
        // .withZoneSameInstant(ZoneId.of("Z"));
        //
        // assertExpression(expected, values, "attimezone(b, 'Z')");
        //
        // assertExpression(ZonedDateTime.parse("2000-10-10T10:10:10Z"), values, "attimezone(c, 'Z')");
    }
}
