package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.nvv;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.core.physicalplan.DatasourceOptions;

/** Test {@link OpenMapCollectionFunction} */
public class OpenMapCollectionFunctionTest extends APhysicalPlanTest
{
    private final TableFunctionInfo openMapCollection = SystemCatalog.get()
            .getTableFunction("open_map_collection");

    @Test
    public void test()
    {
        // "select * from open_map_collection(@col['attribute1']['buckets'])";
        context.setVariable("col", ofEntries(entry("attribute1",
                ofEntries(entry("buckets", asList(ofEntries(true, entry("key", 10), entry("count", 20)), ofEntries(true, entry("key", 11), entry("count", 15), entry("id", "value"))))))));

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
        //@formatter:of
        TupleIterator it = openMapCollection.execute(context, "", asList(arg), new DatasourceOptions(emptyList()));

        int count = 0;
        while (it.hasNext())
        {
            TupleVector vector = it.next();
            assertEquals(Schema.of(Column.of("key", ResolvedType.of(Type.Any)), Column.of("count", ResolvedType.of(Type.Any)), Column.of("id", ResolvedType.of(Type.Any))), vector.getSchema());

            assertVectorsEquals(nvv(Type.Any, 10, 11), vector.getColumn(0));
            assertVectorsEquals(nvv(Type.Any, 20, 15), vector.getColumn(1));
            assertVectorsEquals(vv(Type.Any, null, "value"), vector.getColumn(2));
            count++;
        }

        assertEquals(1, count);
    }
}
