package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/** A scan operator returning constant values for one or more rows. */
public class ConstantScan implements IPhysicalPlan
{
    private final int nodeId;
    private final Schema schema;
    private final boolean hasAsteriskSchemaOrInput;
    private final List<List<IExpression>> rowsExpressions;
    private final TupleVector vector;

    public ConstantScan(int nodeId, Schema schema, List<List<IExpression>> rowsExpressions)
    {
        this.nodeId = nodeId;
        this.schema = schema;
        this.rowsExpressions = requireNonNull(rowsExpressions);
        this.vector = null;
        this.hasAsteriskSchemaOrInput = SchemaUtils.originatesFromAsteriskInput(schema);
    }

    public ConstantScan(int nodeId, TupleVector vector)
    {
        this.nodeId = nodeId;
        this.schema = null;
        this.rowsExpressions = null;
        this.vector = requireNonNull(vector);
        this.hasAsteriskSchemaOrInput = false;
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Constant Scan";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        if (vector != null)
        {
            if (!Schema.EMPTY.equals(vector.getSchema()))
            {
                properties.put("Rows", IntStream.range(0, vector.getRowCount())
                        .mapToObj(i -> "Row " + i
                                       + ": "
                                       + (IntStream.range(0, vector.getSchema()
                                               .getSize())
                                               .mapToObj(j -> "" + vector.getColumn(j)
                                                       .valueAsObject(i))
                                               .collect(joining(", "))))
                        .collect(joining(System.lineSeparator())));
            }
        }
        else
        {
            if (!Schema.EMPTY.equals(schema))
            {
                properties.put("Rows", IntStream.range(0, rowsExpressions.size())
                        .mapToObj(i -> "Row " + i
                                       + ": "
                                       + rowsExpressions.get(i)
                                               .stream()
                                               .map(Object::toString)
                                               .collect(joining(", ")))
                        .collect(joining(System.lineSeparator())));
            }
        }

        return properties;
    }

    @Override
    public Schema getSchema()
    {
        return vector != null ? vector.getSchema()
                : schema;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        if (vector != null)
        {
            return TupleIterator.singleton(vector);
        }

        return TupleIterator.singleton(vectorize(schema, rowsExpressions, context, hasAsteriskSchemaOrInput));
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return emptyList();
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
        else if (obj instanceof ConstantScan)
        {
            ConstantScan that = (ConstantScan) obj;
            return nodeId == that.nodeId;
        }
        return false;
    }

    @Override
    public String toString()
    {
        if (vector != null
                && Schema.EMPTY.equals(vector.getSchema()))
        {
            return vector.getRowCount() == 1 ? "Constant Scan (" + nodeId + "): (Single row)"
                    : "Constant Scan (" + nodeId + "):  (No rows)";
        }
        return "Constant scan (" + nodeId + ")";
    }

    /**
     * Turn a schema and a set of expressions into a {@link TupleVector}.
     */
    public static TupleVector vectorize(Schema schema, List<List<IExpression>> rowsExpressions, IExecutionContext context, boolean hasAsteriskSchemaOrInput)
    {
        int rowSize = rowsExpressions.size();
        int columnSize = rowsExpressions.get(0)
                .size();

        if (schema.getSize() != columnSize)
        {
            throw new QueryException("Schema column count must match row expressions count");
        }

        List<MutableValueVector> values = new ArrayList<>(Collections.nCopies(columnSize, null));

        for (int i = 0; i < columnSize; i++)
        {
            if (!hasAsteriskSchemaOrInput)
            {
                MutableValueVector mutableVector = context.getVectorFactory()
                        .getMutableVector(schema.getColumns()
                                .get(i)
                                .getType(), rowSize);
                values.set(i, mutableVector);
            }

            for (int j = 0; j < rowSize; j++)
            {
                ValueVector result = rowsExpressions.get(j)
                        .get(i)
                        .eval(context);

                MutableValueVector mutableVector = values.get(i);
                if (mutableVector == null)
                {
                    mutableVector = context.getVectorFactory()
                            .getMutableVector(result.type(), rowSize);
                    values.set(i, mutableVector);
                }

                mutableVector.copy(j, result);
            }
        }

        // If we have asterisk input then we need to re-construct the schema from the actual vectors
        if (hasAsteriskSchemaOrInput)
        {
            List<Column> columns = schema.getColumns();
            schema = new Schema(IntStream.range(0, schema.getSize())
                    .mapToObj(i -> (Column) CoreColumn.Builder.from(columns.get(i))
                            .withResolvedType(values.get(i)
                                    .type())
                            .build())
                    .toList());
        }

        return TupleVector.of(schema, values);
    }
}
