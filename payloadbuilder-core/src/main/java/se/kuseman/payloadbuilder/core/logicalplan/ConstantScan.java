package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/** {@link se.kuseman.payloadbuilder.core.physicalplan.ConstantScan} */
public class ConstantScan implements ILogicalPlan
{
    /** One row empty schema. */
    public static final ConstantScan ONE_ROW_EMPTY_SCHEMA = new ConstantScan(Schema.EMPTY, List.of(emptyList()), null);
    public static final ConstantScan ZERO_ROWS_EMPTY_SCHEMA = new ConstantScan(Schema.EMPTY, emptyList(), null);

    private final Schema schema;
    private final List<List<IExpression>> rowsExpressions;
    private final Location location;

    public ConstantScan(Schema schema, List<List<IExpression>> rowsExpressions, Location location)
    {
        this.schema = requireNonNull(schema, "schema");
        this.rowsExpressions = requireNonNull(rowsExpressions, "rowsExpressions");
        this.location = location;
        validate();
    }

    public Location getLocation()
    {
        return location;
    }

    public List<List<IExpression>> getRowsExpressions()
    {
        return rowsExpressions;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public List<ILogicalPlan> getChildren()
    {
        return emptyList();
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return 0;
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
        else if (obj instanceof ConstantScan that)
        {
            return schema.equals(that.schema)
                    && rowsExpressions.equals(that.rowsExpressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        if (ONE_ROW_EMPTY_SCHEMA.equals(this))
        {
            return "Constant Scan (Single row)";
        }
        else if (ZERO_ROWS_EMPTY_SCHEMA.equals(this))
        {
            return "Constant Scan (No rows)";
        }

        return "Constant Scan" + (rowsExpressions.isEmpty() ? ""
                : " (" + rowsExpressions + ")");
    }

    private void validate()
    {
        if (!rowsExpressions.isEmpty())
        {
            int count = rowsExpressions.get(0)
                    .size();

            if (count != schema.getSize())
            {
                throw new ParseException("Rows expressions size must equal column names size.", location);
            }

            for (int i = 1; i < rowsExpressions.size(); i++)
            {
                if (count != rowsExpressions.get(i)
                        .size())
                {
                    throw new ParseException("All rows expressions must be of equal size", location);
                }
            }
        }
    }

    // /** Create a constant scan from provided rows expressions. */
    // public static ConstantScan create(List<String> columnNames, List<List<IExpression>> rowsExpressions, Location location)
    // {
    // if (rowsExpressions == null
    // || rowsExpressions.isEmpty())
    // {
    // return ConstantScan.EMPTY_SCHEMA;
    // }
    //
    // int count = rowsExpressions.get(0)
    // .size();
    // if (count == 0)
    // {
    // throw new ParseException("Rows expressions must not be empty.", location);
    // }
    // else if (count != columnNames.size())
    // {
    // throw new ParseException("Rows expressions size must equal column names size.", location);
    // }
    //
    // for (int i = 1; i < rowsExpressions.size(); i++)
    // {
    // if (count != rowsExpressions.get(i)
    // .size())
    // {
    // throw new ParseException("All rows expressions must be of equal size", location);
    // }
    // }
    //
    // List<ResolvedType> columnTypes = new ArrayList<>(columnNames.size());
    // int rowCount = rowsExpressions.size();
    // List<List<IExpression>> columnsExpressions = new ArrayList<>(count);
    // for (int i = 0; i < count; i++)
    // {
    // columnTypes.add(ResolvedType.ANY);
    //
    // List<IExpression> columnExpressions = new ArrayList<>(rowCount);
    // for (int j = 0; j < rowCount; j++)
    // {
    // IExpression expression = rowsExpressions.get(j)
    // .get(i);
    // if (expression.getType()
    // .getType()
    // .getPrecedence() > columnTypes.get(i)
    // .getType()
    // .getPrecedence())
    // {
    // columnTypes.set(i, expression.getType());
    // }
    //
    // columnExpressions.add(expression);
    // }
    // columnsExpressions.add(columnExpressions);
    // }
    //
    // Schema schema = new Schema(IntStream.range(0, count)
    // .mapToObj(i -> new Column(columnNames.get(i), columnTypes.get(i)))
    // .toList());
    // return new ConstantScan(schema, columnsExpressions);
    // }
}
