package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test {@link ToTableFunction} */
public class ToTableFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo function = SystemCatalog.get()
            .getScalarFunction("totable");

    @Test
    public void test()
    {
        // "select * from open_map_collection((@col).attribute1.buckets)";
        //@formatter:off
        context.setVariable("col",
                ValueVector.literalAny(
                ofEntries(
                    entry("attribute1",
                        ofEntries(
                            entry("buckets", asList(
                                ofEntries(true, entry("key", 10), entry("count", 20)),
                                ofEntries(true, entry("key", 11), entry("count", 15), entry("id", "value")))))))));
        //@formatter:on

        //@formatter:off
        IExpression arg = new DereferenceExpression(
            new DereferenceExpression(
                new VariableExpression(QualifiedName.of("col")),
                "attribute1",
                -1,
                ResolvedType.of(Type.Any)),
            "buckets",
            -1,
            ResolvedType.of(Type.Any));
        //@formatter:on

        Schema expectedSchema = Schema.of(Column.of("key", ResolvedType.of(Type.Any)), Column.of("count", ResolvedType.of(Type.Any)), Column.of("id", ResolvedType.of(Type.Any)));

        ValueVector actual = function.evalScalar(context, TupleVector.CONSTANT, "", asList(arg));

        //@formatter:off
        VectorTestUtils.assertVectorsEquals(vv(ResolvedType.table(Schema.EMPTY),
                TupleVector.of(expectedSchema, asList(
                        vv(Type.Any, 10, 11),
                        vv(Type.Any, 20, 15),
                        vv(Type.Any, null, "value")
                        ))
                ), actual);
        //@formatter:on
    }

    @Test
    public void test_map()
    {
        TupleVector input = TupleVector.of(Schema.of(Column.of("col", ResolvedType.of(Type.Any))), asList(vv(Type.Any, null, ofEntries(entry("key", 123)))));

        Schema expectedSchema = Schema.of(Column.of("key", ResolvedType.of(Type.Any)));

        ValueVector actual = function.evalScalar(context, input, "", asList(ce("col")));

        //@formatter:off
        VectorTestUtils.assertVectorsEquals(vv(ResolvedType.table(Schema.EMPTY),
                null,
                TupleVector.of(expectedSchema, asList(vv(Type.Any, 123)))
                ), actual);
        //@formatter:on
    }
}
