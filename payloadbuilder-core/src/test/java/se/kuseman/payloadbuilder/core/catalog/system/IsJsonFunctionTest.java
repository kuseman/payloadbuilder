package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
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
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link IsJsonFunction} */
public class IsJsonFunctionTest extends APhysicalPlanTest
{
    ScalarFunctionInfo isjson = SystemCatalog.get()
            .getScalarFunction("isjson");
    ScalarFunctionInfo isjsonobject = SystemCatalog.get()
            .getScalarFunction("isjsonobject");
    ScalarFunctionInfo isjsonarray = SystemCatalog.get()
            .getScalarFunction("isjsonarray");

    @Test
    public void test_basic()
    {
        assertEquals(ResolvedType.of(Type.Boolean), isjson.getType(asList(intLit(1))));
        assertEquals(Arity.ONE, isjson.arity());
        assertEquals(ResolvedType.of(Type.Boolean), isjsonobject.getType(asList(intLit(1))));
        assertEquals(Arity.ONE, isjsonobject.arity());
        assertEquals(ResolvedType.of(Type.Boolean), isjsonarray.getType(asList(intLit(1))));
        assertEquals(Arity.ONE, isjsonarray.arity());
    }

    @Test
    public void test_isjson()
    {
        //@formatter:off
        Object[] values = new Object[]   {1,    2,    true, false, null, "null", "no", "{}",  "[]", "{\"key\":123}", "[1,2,3]", "{",   ""};
        Object[] results = new Boolean[] {true, true, true, true,  null, true,   false, true, true, true,            true,      false, false};
        //@formatter:on

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", ResolvedType.of(Type.Any))), asList(vv(Type.Any, values)));

        ValueVector actual;

        actual = isjson.evalScalar(context, input, "", asList(ce("col1")));
        assertVectorsEquals(vv(Type.Boolean, results), actual);
    }

    @Test
    public void test_isobject()
    {
        //@formatter:off
        Object[] values = new Object[]   {1,     2,     true,  false,  null, "null",  "no", "{}",  "[]",  "{\"key\":123}", "[1,2,3]", "{",    ""};
        Object[] results = new Boolean[] {false, false, false, false,  null, true,    false, true, false, true,            false,      false, false};
        //@formatter:on

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", ResolvedType.of(Type.Any))), asList(vv(Type.Any, values)));

        ValueVector actual;

        actual = isjsonobject.evalScalar(context, input, "", asList(ce("col1")));
        assertVectorsEquals(vv(Type.Boolean, results), actual);
    }

    @Test
    public void test_isarray()
    {
        //@formatter:off
        Object[] values = new Object[]   {1,     2,     true,  false,  null, "null",  "no", "{}",   "[]",  "{\"key\":123}", "[1,2,3]", "{",   ""};
        Object[] results = new Boolean[] {false, false, false, false,  null, true,    false, false, true,  false,           true,      false, false};
        //@formatter:on

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", ResolvedType.of(Type.Any))), asList(vv(Type.Any, values)));

        ValueVector actual;

        actual = isjsonarray.evalScalar(context, input, "", asList(ce("col1")));
        assertVectorsEquals(vv(Type.Boolean, results), actual);
    }
}
