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
        String[] columns = new String[] {"pos", "col1", "col2", "col3"};
        Row r1 = Row.of(alias, columns, new Object[] {666, 1, 2, 3});
        assertRow(r1);
        Row r1o = (Row) r1.optimize(context);
        assertRow(r1o);
        assertSame(r1o, r1o.optimize(context));

        r1 = Row.of(alias, columns, new Row.MapValues(MapUtils.ofEntries(
                MapUtils.entry("pos", 666),
                MapUtils.entry("col1", 1),
                MapUtils.entry("col2", 2),
                MapUtils.entry("col3", 3)), columns));

        assertRow(r1);
        r1o = (Row) r1.optimize(context);
        assertNotSame(r1, r1o);
        assertRow(r1o);
    }

    private void assertRow(Row r1)
    {
        assertEquals(0, r1.getTupleOrdinal());
        assertEquals(r1, r1.getTuple(0));
        assertNull(r1.getTuple(1));
        assertEquals(4, r1.getColumnCount());
        assertEquals(0, r1.getColumnOrdinal("pos"));
        assertEquals(1, r1.getColumnOrdinal("col1"));
        assertEquals(2, r1.getColumnOrdinal("col2"));
        assertEquals(3, r1.getColumnOrdinal("col3"));
        assertEquals("pos", r1.getColumn(0));
        assertEquals("col1", r1.getColumn(1));
        assertEquals("col2", r1.getColumn(2));
        assertEquals("col3", r1.getColumn(3));
        assertEquals(3, r1.getValue(3));
        assertEquals(2, r1.getValue(2));
        assertEquals(666, r1.getValue(0));
        assertNull(r1.getValue(-1));
        assertEquals(1, r1.getValue(1));
        assertEquals(666, r1.getValue(0));
    }
}
