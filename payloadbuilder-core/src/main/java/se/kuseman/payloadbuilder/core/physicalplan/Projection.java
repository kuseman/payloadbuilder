package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;

/** Projection of a plan that projects a list of expression */
public class Projection implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final List<IExpression> expressions;
    private final Schema schema;
    private final boolean hasAsteriskProjection;
    private final boolean hasAsteriskSchema;
    private final TableSourceReference parentTableSource;

    public Projection(int nodeId, IPhysicalPlan input, List<IExpression> expressions, TableSourceReference parentTableSource)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.expressions = requireNonNull(expressions, "expressions");
        this.schema = SchemaUtils.getSchema(parentTableSource, expressions, false);
        this.hasAsteriskProjection = expressions.stream()
                .anyMatch(e -> e instanceof AsteriskExpression);
        this.hasAsteriskSchema = hasAsteriskProjection
                || SchemaUtils.isAsterisk(schema);
        this.parentTableSource = parentTableSource;
    }

    public IPhysicalPlan getInput()
    {
        return input;
    }

    public List<IExpression> getExpressions()
    {
        return expressions;
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
        return schema;
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
                Schema outerSchema = outerTupleVector != null ? outerTupleVector.getSchema()
                        : null;
                List<IExpression> actualExpressions = hasAsteriskProjection ? ProjectionUtils.expandExpressions(expressions, outerSchema, vector.getSchema())
                        : expressions;
                final int expressionSize = actualExpressions.size();
                final ValueVector[] vectors = new ValueVector[expressionSize];
                for (int i = 0; i < expressionSize; i++)
                {
                    vectors[i] = actualExpressions.get(i)
                            .eval(vector, context);
                }
                final Schema schema = hasAsteriskSchema ? SchemaUtils.getSchema(parentTableSource, actualExpressions, vectors, false)
                        : Projection.this.schema;
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
                        return vectors[column];
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
        else if (obj instanceof Projection that)
        {
            return nodeId == that.nodeId
                    && input.equals(that.input)
                    && expressions.equals(that.expressions)
                    && Objects.equals(parentTableSource, that.parentTableSource);
        }
        return false;
    }

    @Override
    public String toString()
    {
        // Use verbose string in plan printing
        return "Projection: " + "("
               + nodeId
               + "):  expressions: "
               + expressions.stream()
                       .map(i -> i.toVerboseString())
                       .collect(joining(", "));
    }
}
