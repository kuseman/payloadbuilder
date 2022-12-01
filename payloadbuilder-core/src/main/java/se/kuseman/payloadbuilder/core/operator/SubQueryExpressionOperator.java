package se.kuseman.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/**
 * <pre>
 * Operator that sits before a sub queries FROM operator
 * to return a hierarchy tuple with context to be able to traverse
 * upwards in hierarchy.
 *
 * ie.
 * select
 * (
 *      select a.col + b.col newCol
 *      from open_rows(b)
 *      order by newCol
 *      for object_array
 * )
 * from tableA a
 * inner join tableB b with (populate=true)
 *   on ....
 *
 * This sub query from operator (open_rows) returns rows of type tableB
 * but from that tuple there is no way to traverse to a.col which is needed
 * by the computed expression.
 *
 * And then this operator comes into play and sits in the oeprator tree like below:
 *
 *   OrderByOperator (newCol)
 *     ComputedColumns (newCol = a.col+ b.col)
 *       SubQueryExpressionOperator
 *         TablFunction (open_rows(b))
 *
 * And returns a {@link HierarchyTuple} which connects the FROM operators tuple
 * with the context
 * </pre>
 */
class SubQueryExpressionOperator extends AOperator
{
    private final Operator target;
    private final TableAlias alias;

    /**
     * @param nodeId Id of the node
     * @param alias The (optional) alias that this subquery targets. This is used to know if parent tuple should be delegated to or not when asking for a tuple ordinal.
     * @param target Target operator
     */
    SubQueryExpressionOperator(int nodeId, TableAlias alias, Operator target)
    {
        super(nodeId);
        this.alias = alias;
        this.target = requireNonNull(target, "target");
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        // Delegate to target
        return target.getDescribeProperties(context);
    }

    @Override
    public TupleIterator open(IExecutionContext ctx)
    {
        ExecutionContext context = (ExecutionContext) ctx;
        final Tuple contextTuple = context.getStatementContext()
                .getTuple();
        // CSOFF
        return new ATupleIterator(target.open(context))
        // CSON
        {
            private HierarchyTuple hierarchyTuple;

            @Override
            public Tuple process(Tuple tuple)
            {
                // No op tuple, no need to create a hierarchy tuple
                // just return the context tuple
                if (tuple == NoOpTuple.NO_OP)
                {
                    return contextTuple;
                }
                else
                {
                    if (hierarchyTuple == null)
                    {
                        int maxTupleOrdinal = alias != null ? alias.getMaxTupleOrdinal()
                                : -1;
                        hierarchyTuple = new HierarchyTuple(maxTupleOrdinal, contextTuple);
                    }
                    hierarchyTuple.setCurrent(tuple);
                    return hierarchyTuple;
                }
            }
        };
    }

    @Override
    public int hashCode()
    {
        return target.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SubQueryExpressionOperator)
        {
            SubQueryExpressionOperator that = (SubQueryExpressionOperator) obj;

            int thisTupleOrdinal = alias != null ? alias.getTupleOrdinal()
                    : -1;
            int thatTupleOrdinal = that.alias != null ? that.alias.getTupleOrdinal()
                    : -1;

            return nodeId.equals(that.nodeId)
                    && thisTupleOrdinal == thatTupleOrdinal
                    && target.equals(that.target);
        }
        return false;
    }

    /**
     * Tuple that is used to temporary connect 2 tuples during a complex value iteration
     *
     * <pre>
     *  select a.col,
     *         (select a.col2, b.col3 from tableB for array) arr
     *  from tableA a
     *
     *  To be able to "reach" alias b from within the complex value arr
     *  we need to temporary connect the outer tuple (tableA) before evaluating
     * </pre>
     **/
    static class HierarchyTuple implements Tuple
    {
        private final int maxTupleOrdinal;
        private final Tuple parent;
        private Tuple current;

        HierarchyTuple(int maxTupleOrdinal, Tuple parent)
        {
            this.maxTupleOrdinal = maxTupleOrdinal;
            this.parent = parent;
        }

        void setCurrent(Tuple current)
        {
            this.current = current;
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            if (current == null)
            {
                return -1;
            }
            return current.getColumnOrdinal(column);
        }

        @Override
        public Tuple getTuple(int tupleOrdinal)
        {
            /*@formatter:off
             * select col ,
             * (
             *   select a.col1, b.col2, c.col
             *   from tableB b                           ordinal = 1 (current)
             *   inner join tableC c                     ordinal = 2
             *   for object
             * ) obj
             * from tableA a                             ordinal = 0 (parent)
             *
             * select
             * (
             *   id                        <------ 1
             *   , c.id                    <------ 2
             *   from open_rows(b)         <------ 1 (current)
             * ) obj
             * from tableA a               0 (composite parent)
             * inner join tableB b         1
             *   on ...
             * inner join tableC c         2 on ...
             *@formatter:on
             */
            if (tupleOrdinal == -1
                    || current == null
                    || tupleOrdinal == current.getTupleOrdinal())
            {
                return current;
            }

            // Target might be a descendant of current
            if (tupleOrdinal > current.getTupleOrdinal())
            {
                Tuple result = current.getTuple(tupleOrdinal);
                if (result != null)
                {
                    return result;
                }

                // If the target ordinal is supposed to be below current
                // then return result since we don't need to ask parent
                if (maxTupleOrdinal >= 0
                        && tupleOrdinal <= maxTupleOrdinal)
                {
                    return result;
                }

            }
            // if not found delegate to parent
            return parent.getTuple(tupleOrdinal);
        }

        @Override
        public Object getValue(int columnOrdinal)
        {
            if (current == null)
            {
                return null;
            }
            return current.getValue(columnOrdinal);
        }

        @Override
        public int getTupleOrdinal()
        {
            if (current == null)
            {
                return -1;
            }
            return current.getTupleOrdinal();
        }

        @Override
        public int getColumnCount()
        {
            throw new IllegalArgumentException("Not implemented");
        }

        @Override
        public String getColumn(int columnOrdinal)
        {
            throw new IllegalArgumentException("Not implemented");
        }
    }
}
