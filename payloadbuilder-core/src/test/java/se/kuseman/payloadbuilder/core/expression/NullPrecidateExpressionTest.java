package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link NullPredicateExpression} */
public class NullPrecidateExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test()
    {
        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", ResolvedType.of(Type.Int)), Column.of("col2", ResolvedType.of(Type.Any))),
                asList(VectorTestUtils.vv(Type.Int, 1, 2, null), VectorTestUtils.vv(Type.Any, emptyMap(), emptyMap(), MapUtils.ofEntries(MapUtils.entry("key", "value")))));

        NullPredicateExpression e = new NullPredicateExpression(ce("col1"), false);

        ValueVector actual;

        actual = e.eval(input, context);
        assertVectorsEquals(vv(Type.Boolean, false, false, true), actual);

        e = new NullPredicateExpression(ce("col1"), true);

        actual = e.eval(input, context);
        assertVectorsEquals(vv(Type.Boolean, true, true, false), actual);

        // Special case for Type = ANY
        e = new NullPredicateExpression(new DereferenceExpression(ce("col2"), "key", -1, ResolvedType.of(Type.Any)), false);
        actual = e.eval(input, context);
        assertVectorsEquals(vv(Type.Boolean, true, true, false), actual);
    }
}
