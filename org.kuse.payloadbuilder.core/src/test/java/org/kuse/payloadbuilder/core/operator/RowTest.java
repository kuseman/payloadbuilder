package org.kuse.payloadbuilder.core.operator;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.TableAlias.Type;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Test of {@link Row} */
public class RowTest extends AOperatorTest
{
    private final TableAlias alias = TableAliasBuilder.of(0, Type.TABLE, QualifiedName.of("table"), "a").build();

    @Test
    public void test()
    {
        String[] columns = new String[] {"col1", "col2", "col3"};
        Row r1 = Row.of(alias, 666, columns, new Object[] {1, 2, 3});
        assertRow(r1);
        Row r1o = (Row) r1.optimize(context);
        assertRow(r1o);
        assertSame(r1o, r1o.optimize(context));

        r1 = Row.of(alias, 666, columns, new Row.MapValues(MapUtils.ofEntries(
                MapUtils.entry("col1", 1),
                MapUtils.entry("col2", 2),
                MapUtils.entry("col3", 3)), columns));

        assertRow(r1);
        r1o = (Row) r1.optimize(context);
        assertNotSame(r1,  r1o);
        assertRow(r1o);
    }

    private void assertRow(Row r1)
    {
        assertEquals(0, r1.getTupleOrdinal());
        assertEquals(r1, r1.getTuple(0));
        assertNull(r1.getTuple(1));
        assertEquals(3, r1.getColumnCount());
        assertEquals(2, r1.getColumnOrdinal("col3"));
        assertEquals(1, r1.getColumnOrdinal("col2"));
        assertEquals(Row.POS_ORDINAL, r1.getColumnOrdinal("__pos"));
        assertEquals("col3", r1.getColumn(2));
        assertEquals("col2", r1.getColumn(1));
        assertEquals("col1",r1.getColumn(0));
        assertEquals(3, r1.getValue(2));
        assertEquals(2, r1.getValue(1));
        assertEquals(666, r1.getValue(Row.POS_ORDINAL));
        assertNull(r1.getValue(-1));
        assertEquals(1, r1.getValue(0));
    }
}
