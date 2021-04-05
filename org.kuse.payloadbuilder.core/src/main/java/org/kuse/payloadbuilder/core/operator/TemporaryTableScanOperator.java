package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

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
    public RowIterator open(ExecutionContext context)
    {
        List<Tuple> rows = context.getSession().getTemporaryTable(table.getTable());
        if (rows == null)
        {
            throw new OperatorException("Data for temporary table " + table.getTable() + " could not be found");
        }
        final Iterator<Tuple> it = rows.iterator();
        return new RowIterator()
        {
            @Override
            public Tuple next()
            {
                return new TemporaryTableTuple(it.next(), table.getTableAlias().getTupleOrdinal());
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
     * Tuple that wraps tuples from temporary tables and changes it's tuple ordinal depending on the ordinal it got assign when used in queries
     */
    private static class TemporaryTableTuple implements Tuple
    {
        private final Tuple tuple;
        private final int tupleOrdinal;

        private TemporaryTableTuple(Tuple tuple, int tupleOrdinal)
        {
            this.tuple = tuple;
            this.tupleOrdinal = tupleOrdinal;
        }

        @Override
        public int getTupleOrdinal()
        {
            return tupleOrdinal;
        }

        @Override
        public Tuple getTuple(int tupleOrdinal)
        {
            return tuple.getTuple(getActualTupleOrdinal(tupleOrdinal));
        }

        @Override
        public Object getValue(String column)
        {
            return tuple.getValue(column);
        }

        @Override
        public Iterator<TupleColumn> getColumns(int tupleOrdinal)
        {
            final Iterator<TupleColumn> it = tuple.getColumns(getActualTupleOrdinal(tupleOrdinal));
            // Replace the downstream ordinal with the dynamic one
            //CSOFF
            return new Iterator<Tuple.TupleColumn>()
            //CSON
            {
                @Override
                public TupleColumn next()
                {
                    TupleColumn tc = it.next();
                    return new TupleColumn()
                    {
                        @Override
                        public int getTupleOrdinal()
                        {
                            return TemporaryTableTuple.this.tupleOrdinal;
                        }

                        @Override
                        public String getColumn()
                        {
                            return tc.getColumn();
                        }
                    };
                }

                @Override
                public boolean hasNext()
                {
                    return it.hasNext();
                }
            };
        }

        private int getActualTupleOrdinal(int tupleOrdinal)
        {
            // If the tuple ordinal wanted is the dynamic one assign then transform back to the original tuples
            // ordinal
            return tupleOrdinal == this.tupleOrdinal ? tuple.getTupleOrdinal() : tupleOrdinal;
        }
    }
}
