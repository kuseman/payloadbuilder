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
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Test of {@link ComputedColumnsOperator} */
public class ComputedColumnsOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        Random rnd = new Random();
        TableAlias alias = TableAliasBuilder.of(-1, TableAlias.Type.TABLE, QualifiedName.of("table"), "a")
                .tableMeta(new TableMeta(asList(new TableMeta.Column("col1", DataType.ANY))))
                .build();
        MutableBoolean close = new MutableBoolean();
        Operator target = op(ctx -> IntStream.range(0, 100)
                .mapToObj(i -> (Tuple) Row.of(alias, new Object[] { rnd.nextInt(100) }))
                .iterator(), () -> close.setTrue());
        ComputedColumnsOperator operator = new ComputedColumnsOperator(0, -1, target, new ExpressionOrdinalValuesFactory(asList(e("concat('v-', col1)"))));

        TupleIterator it = operator.open(new ExecutionContext(session));
        while (it.hasNext())
        {
            Tuple tuple = it.next();

            assertEquals(1, tuple.getColumnCount());
            assertEquals("col1", tuple.getColumn(0));
            assertEquals(0, tuple.getColumnOrdinal("COL1"));

            int val = (int) getValue(tuple, -1, "col1");
            Object actual = getValue(tuple, -1, TableMeta.MAX_COLUMNS);
            assertEquals("v-" + val, actual);
        }
        it.close();
        assertTrue(close.booleanValue());
    }

    @Test
    public void test_1()
    {
        Random rnd = new Random();
        TableAlias alias = TableAliasBuilder.of(-1, TableAlias.Type.TABLE, QualifiedName.of("table"), "a")
                .tableMeta(new TableMeta(asList(new TableMeta.Column("col1", DataType.ANY))))
                .build();
        MutableBoolean close = new MutableBoolean();
        Operator target = op(ctx -> IntStream.range(0, 100)
                .mapToObj(i -> (Tuple) Row.of(alias, new Object[] { rnd.nextInt(100) }))
                .iterator(), () -> close.setTrue());
        ComputedColumnsOperator operator = new ComputedColumnsOperator(0, -1, target, new ExpressionOrdinalValuesFactory(asList(e("concat('v-', col1)"))));

        TupleIterator it = operator.open(new ExecutionContext(session));
        while (it.hasNext())
        {
            Tuple tuple = it.next();

            assertEquals(1, tuple.getColumnCount());
            assertEquals("col1", tuple.getColumn(0));
            assertEquals(0, tuple.getColumnOrdinal("COL1"));

            int val = (int) getValue(tuple, -1, "col1");
            Object actual = getValue(tuple, -1, TableMeta.MAX_COLUMNS);
            assertEquals("v-" + val, actual);
        }
        it.close();
        assertTrue(close.booleanValue());
    }
}
