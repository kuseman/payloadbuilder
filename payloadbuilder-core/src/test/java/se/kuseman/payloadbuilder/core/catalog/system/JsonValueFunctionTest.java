package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link JsonValueFunction} */
public class JsonValueFunctionTest extends APhysicalPlanTest
{
    ScalarFunctionInfo f = SystemCatalog.get()
            .getScalarFunction("json_value");

    @Test
    public void test_basic()
    {
        assertEquals(ResolvedType.of(Type.Any), f.getType(asList(intLit(1))));
        assertEquals(Arity.ONE, f.arity());
    }

    @Test
    public void test()
    {
        ValueVector actual;

        IExpression col1 = ce("col1", ResolvedType.of(Type.Int));
        IExpression col2 = ce("col2", ResolvedType.of(Type.String));
        IExpression col3 = ce("col3", ResolvedType.of(Type.String));
        //
        //@formatter:off
        Schema schema = Schema.of(
                Column.of("col1", ResolvedType.of(Type.Int)),
                Column.of("col2", ResolvedType.of(Type.String)),
                Column.of("col3", ResolvedType.of(Type.String))
                );
                
        TupleVector input = TupleVector.of(schema, asList(
                vv(Type.Int, null, 2, 4, 5, 6, 7),
                vv(Type.String, "{\"key\":123}", "true", null, "[1,2,3]", "[{\"key\":1}, {\"key\":2}]", "null"),
                vv(Type.String, "broken", "json", null, null, null, null)
                ));
        //@formatter:on

        actual = f.evalScalar(context, input, "", asList(col1));
        assertVectorsEquals(vv(Type.Any, null, 2, 4, 5, 6, 7), actual);

        actual = f.evalScalar(context, input, "", asList(col2));
        assertVectorsEquals(vv(Type.Any, ofEntries(entry("key", 123)), true, null, asList(1, 2, 3), asList(ofEntries(entry("key", 1)), ofEntries(entry("key", 2))), null), actual);

        try
        {
            actual = f.evalScalar(context, input, "", asList(col3));
            actual.getAny(0);
            fail("Should fail cause of broken json");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Error deserializing 'broken'"));
        }
    }
}
