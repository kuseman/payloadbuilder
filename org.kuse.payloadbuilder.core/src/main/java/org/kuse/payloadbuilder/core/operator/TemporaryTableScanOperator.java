package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.operator.TemporaryTable.Row;
import org.kuse.payloadbuilder.core.parser.Table;

/** Temporary table scan operator */
public class TemporaryTableScanOperator extends AOperator
{
    private final Table table;

    public TemporaryTableScanOperator(int nodeId, Table table)
    {
        super(nodeId);
        this.table = requireNonNull(table, "table");
    }

    @Override
    public String getName()
    {
        return "scan (#" + table.getTable() + ")";
    }

    @Override
    public TupleIterator open(ExecutionContext context)
    {
        TemporaryTable temporaryTable = context.getSession().getTemporaryTable(table.getTable());
        if (table == null)
        {
            throw new OperatorException("Data for temporary table " + table.getTable() + " could not be found");
        }
        final Iterator<TemporaryTable.Row> it = temporaryTable.getRows().iterator();
        return new TupleIterator()
        {
            @Override
            public Tuple next()
            {
                Row row = it.next();
                return new TemporaryTableTuple(table.getTableAlias().getTupleOrdinal(), row.getTuple(), temporaryTable.getColumns(), row.getValues());
            }

            @Override
            public boolean hasNext()
            {
                return it.hasNext();
            }
        };
    }

    @Override
    public int hashCode()
    {
        return table.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TemporaryTableScanOperator)
        {
            TemporaryTableScanOperator that = (TemporaryTableScanOperator) obj;
            return table.getTable().equals(that.table.getTable())
                && table.isTempTable() == that.table.isTempTable()
                && table.getTableAlias().getTupleOrdinal() == that.table.getTableAlias().getTupleOrdinal()
                && Objects.equals(table.getCatalogAlias(), that.table.getCatalogAlias());
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "#" + table.getTable().toString();
    }

    /**
     * <pre>
     * Tuple that wraps tuples from temporary tables and changes it's
     * tuple ordinal depending on the ordinal it got assign when used in queries
     * </pre>
     */
    private static class TemporaryTableTuple implements Tuple
    {
        private final int tupleOrdinal;
        private final Tuple tuple;
        private final String[] columns;
        private final Object[] values;

        private TemporaryTableTuple(int tupleOrdinal, Tuple tuple, String[] columns, Object[] values)
        {
            this.tupleOrdinal = tupleOrdinal;
            this.tuple = requireNonNull(tuple);
            this.columns = requireNonNull(columns);
            this.values = requireNonNull(values);
        }

        @Override
        public int getTupleOrdinal()
        {
            return tupleOrdinal;
        }

        @Override
        public int getColumnCount()
        {
            return columns.length;
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            return ArrayUtils.indexOf(columns, column);
        }

        @Override
        public String getColumn(int columnOrdinal)
        {
            return columns[columnOrdinal];
        }

        @Override
        public Tuple getTuple(int tupleOrdinal)
        {
            if (tupleOrdinal == this.tupleOrdinal)
            {
                return this;
            }
            return tuple.getTuple(tupleOrdinal);
        }

        @Override
        public Object getValue(int ordinal)
        {
            if (ordinal == -1)
            {
                return null;
            }

            return values[ordinal];
        }
    }
}
