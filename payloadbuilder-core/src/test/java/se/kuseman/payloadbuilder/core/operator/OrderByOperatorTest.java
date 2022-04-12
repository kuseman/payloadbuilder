package se.kuseman.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableAlias.TableAliasBuilder;
import se.kuseman.payloadbuilder.api.TableMeta;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.parser.SortItem;

/** Test of {@link OrderByOperator} */
public class OrderByOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        Random rnd = new Random();
        TableAlias alias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("table"), "a")
                .tableMeta(new TableMeta(asList(new TableMeta.Column("col1", DataType.ANY))))
                .build();
        MutableBoolean close = new MutableBoolean();
        Operator target = op(ctx -> IntStream.range(0, 100)
                .mapToObj(i -> (Tuple) Row.of(alias, new Object[] { rnd.nextInt(100) }))
                .iterator(), () -> close.setTrue());
        OrderByOperator operator = new OrderByOperator(0, target, new ExpressionTupleComparator(asList(new SortItem(e("col1"), Order.ASC, NullOrder.UNDEFINED, null))));

        TupleIterator it = operator.open(new ExecutionContext(session));
        int prev = -1;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int val = (int) getValue(tuple, 0, "col1");
            if (prev != -1)
            {
                assertTrue(prev <= val);
            }

            prev = val;
        }
        it.close();
        assertTrue(close.booleanValue());
    }
}
