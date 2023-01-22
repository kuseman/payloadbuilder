package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;

/** Projection of a plan that projects a list of expression */
public class Projection implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final List<IExpression> expressions;
    private final boolean hasAsteriskExpressions;
    /** True if input column should be appended to output. Used if thie projection is used to computed new values to input */
    private final boolean appendInputColumns;

    public Projection(int nodeId, IPhysicalPlan input, List<IExpression> expressions, boolean appendInputColumns)
    {
        this.nodeId = nodeId;
        this.appendInputColumns = appendInputColumns;
        this.input = requireNonNull(input, "input");
        this.expressions = requireNonNull(expressions, "expressions");
        this.hasAsteriskExpressions = expressions.stream()
                .anyMatch(e -> e.getColumnReference() != null
                        && e.getColumnReference()
                                .isAsterisk());
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Project";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return ofEntries(true, entry(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(getSchema())), entry(IDatasource.DEFINED_VALUES, expressions.stream()
                .filter(e -> e instanceof AliasExpression
                        && ((AliasExpression) e).isInternal())
                .map(e ->
                {
                    String alias = ((AliasExpression) e).getAliasString();
                    IExpression ee = ((AliasExpression) e).getExpression();
                    return alias + ": " + ee.toString();
                })
                .collect(joining(", "))));
    }

    @Override
    public Schema getSchema()
    {
        return ProjectionUtils.createSchema(input.getSchema(), expressions, appendInputColumns);
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final TupleVector outerTupleVector = ((StatementContext) context.getStatementContext()).getOuterTupleVector();
        final TupleIterator it = input.execute(context);
        return new TupleIterator()
        {
            @Override
            public TupleVector next()
            {
                if (!it.hasNext())
                {
                    throw new NoSuchElementException();
                }

                final TupleVector vector = it.next();

                // if (hasAsteriskExpressions)
                // {
                // System.out.println("has asterisk " + expressions
                // + " "
                // + nodeId
                // + " "
                // + (outerTupleVector != null ? outerTupleVector.getSchema()
                // : null));
                // }
                //
                Schema outerSchema = outerTupleVector != null ? outerTupleVector.getSchema()
                        : null;

                List<IExpression> actualExpressions = hasAsteriskExpressions ? ProjectionUtils.expandExpressions(expressions, outerSchema, vector.getSchema())
                        : expressions;
                // Create the actual schema from the expressions
                // TODO: this should done only once in planning if the input schema is static
                final Schema schema = ProjectionUtils.createSchema(vector.getSchema(), actualExpressions, appendInputColumns);
                final int expressionSize = actualExpressions.size();
                final ValueVector[] vectors = new ValueVector[expressionSize];
                for (int i = 0; i < expressionSize; i++)
                {
                    vectors[i] = actualExpressions.get(i)
                            .eval(vector, context);
                }

                return new TupleVector()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return schema;
                    }

                    @Override
                    public int getRowCount()
                    {
                        return vector.getRowCount();
                    }

                    @Override
                    public ValueVector getColumn(int column)
                    {
                        if (column < expressionSize)
                        {
                            return vectors[column];
                        }

                        // Columns is appended last return column from vecor
                        return vector.getColumn(column - expressionSize);
                    }
                };
            }

            @Override
            public boolean hasNext()
            {
                return it.hasNext();
            }

            @Override
            public void close()
            {
                it.close();
            }
        };
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(input);
    }

    @Override
    public int hashCode()
    {
        return nodeId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof Projection)
        {
            Projection that = (Projection) obj;
            return nodeId == that.nodeId
                    && input.equals(that.input)
                    && expressions.equals(that.expressions)
                    && appendInputColumns == that.appendInputColumns;
        }
        return false;
    }

    @Override
    public String toString()
    {
        // Use verbose string in plan printing
        return (appendInputColumns ? "Compute "
                : "Projection ")
               + "("
               + nodeId
               + "):  expressions: "
               + expressions.stream()
                       .map(i -> i.toVerboseString())
                       .collect(joining(", "));
    }
}
