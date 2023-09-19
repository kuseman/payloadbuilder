package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/** Physical expression scan */
public class ExpressionScan implements IPhysicalPlan
{
    private final int nodeId;
    private final TableSourceReference tableSource;
    private final Schema schema;
    private final IExpression expression;
    private final boolean asteriskSchema;

    public ExpressionScan(int nodeId, TableSourceReference tableSource, Schema schema, IExpression expression)
    {
        this.nodeId = nodeId;
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.schema = requireNonNull(schema, "schema");
        this.expression = requireNonNull(expression, "expression");
        this.asteriskSchema = SchemaUtils.isAsterisk(schema);
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(getSchema()));
        return properties;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        ValueVector vector = expression.eval(context);
        if (vector.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        // Wrap vector and return the planned schema
        final TupleVector table = vector.getTable(0);
        final Schema schema = asteriskSchema ? TableScan.recreateSchema(tableSource, table.getSchema())
                : this.schema;
        return TupleIterator.singleton(new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return table.getRowCount();
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return table.getColumn(column);
            }
        });
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return emptyList();
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof ExpressionScan)
        {
            ExpressionScan that = (ExpressionScan) obj;
            return nodeId == that.nodeId
                    && tableSource.equals(that.tableSource)
                    && schema.equals(that.schema)
                    && expression.equals(that.expression)
                    && asteriskSchema == that.asteriskSchema;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Scan expression: " + expression.toString();
    }
}
