package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test {@link RangeFunction} */
public class RangeFunctionTest extends APhysicalPlanTest
{
    private final TableFunctionInfo f = SystemCatalog.get()
            .getTableFunction("range");

    @Test
    public void test()
    {
        Schema schema = Schema.of(Column.of("Value", ResolvedType.of(Type.Int)));
        TupleIterator it = f.execute(context, "", asList(intLit(1), intLit(10)), new FunctionData(0, emptyList()));

        int count = 0;
        while (it.hasNext())
        {
            TupleVector vector = it.next();
            assertEquals(schema, vector.getSchema());

            assertVectorsEquals(vv(Type.Int, 1, 2, 3, 4, 5, 6, 7, 8, 9), vector.getColumn(0));
            count++;
        }

        assertEquals(1, count);
    }

    @Test
    public void test_batch_size()
    {
        Schema schema = Schema.of(Column.of("Value", ResolvedType.of(Type.Int)));
        context.setVariable("batch", ValueVector.literalInt(3, 1));
        TupleIterator it = f.execute(context, "", asList(intLit(1), intLit(11)), new FunctionData(0, asList(new Option(QualifiedName.of("batch_size"), new VariableExpression("batch")))));

        int count = 0;
        while (it.hasNext())
        {
            TupleVector vector = it.next();
            assertEquals(schema, vector.getSchema());

            if (count == 0)
            {
                assertVectorsEquals(vv(Type.Int, 1, 2, 3), vector.getColumn(0));
            }
            else if (count == 1)
            {
                assertVectorsEquals(vv(Type.Int, 4, 5, 6), vector.getColumn(0));
            }
            else if (count == 2)
            {
                assertVectorsEquals(vv(Type.Int, 7, 8, 9), vector.getColumn(0));
            }
            else
            {
                assertVectorsEquals(vv(Type.Int, 10), vector.getColumn(0));
            }
            count++;
        }

        assertEquals(4, count);
    }
}
