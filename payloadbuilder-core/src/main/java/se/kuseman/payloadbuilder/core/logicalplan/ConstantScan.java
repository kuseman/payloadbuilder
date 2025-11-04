package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/** {@link se.kuseman.payloadbuilder.core.physicalplan.ConstantScan} */
public class ConstantScan implements ILogicalPlan
{
    public static final ConstantScan ONE_ROW_EMPTY_SCHEMA = new ConstantScan(Schema.EMPTY, List.of(emptyList()), null);
    public static final ConstantScan ZERO_ROWS_EMPTY_SCHEMA = new ConstantScan(Schema.EMPTY, emptyList(), null);

    private final Schema schema;
    private final List<List<IExpression>> rowsExpressions;
    private final Location location;

    private ConstantScan(Schema schema, List<List<IExpression>> rowsExpressions, Location location)
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
                throw new ParseException("Count of each row must equal the column count.", location);
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

    /** Re-create this constant scan with a new set of rows expressions */
    public ConstantScan reCreate(List<List<IExpression>> rowsExpressions)
    {
        // Nothing to change here
        if (rowsExpressions.isEmpty())
        {
            return this;
        }

        List<ColumnData> columnDatas = getColumnDataFromRows(null, rowsExpressions, location);
        List<Column> columns = this.schema.getColumns();
        Schema schema = new Schema(IntStream.range(0, columns.size())
                .mapToObj(i -> getColumn(columns.get(i), null, columnDatas.get(i)))
                .toList());
        return new ConstantScan(schema, rowsExpressions, location);
    }

    /** Constructs a constant scan with a schema and no rows. */
    public static ConstantScan create(Schema schema)
    {
        return new ConstantScan(schema, emptyList(), null);
    }

    /** Constructs a constant scan with provided column names and row expressions. */
    public static ConstantScan create(TableSourceReference tableSource, List<String> columnNames, List<List<IExpression>> rowsExpressions, Location location)
    {
        requireNonNull(tableSource);
        // Create schema from provided row expressions. Highest priority of each column
        List<ColumnData> columnDatas = getColumnDataFromRows(tableSource, rowsExpressions, location);
        Schema schema = new Schema(IntStream.range(0, columnNames.size())
                .mapToObj(i -> getColumn(null, columnNames.get(i), columnDatas.get(i)))
                .toList());

        return new ConstantScan(schema, rowsExpressions, location);
    }

    /** Constructs a constant scan with provided column names and a single row of expressions. */
    public static ConstantScan create(List<IExpression> rowExpressions, Location location)
    {
        Schema schema = SchemaUtils.getSchema(null, rowExpressions, false);
        return new ConstantScan(schema, List.of(rowExpressions), location);
    }

    /** Constructs a constant scan with provided rows expressions. */
    public static ConstantScan createFromRows(List<List<IExpression>> rowsExpressions, Location location)
    {
        // Extract the schema from the first row
        List<ColumnData> columnDatas = getColumnDataFromRows(null, rowsExpressions, location);

        Schema schema = new Schema(IntStream.range(0, columnDatas.size())
                .mapToObj(i -> getColumn(null, "column" + i, columnDatas.get(i)))
                .toList());
        return new ConstantScan(schema, rowsExpressions, location);
    }

    private static Column getColumn(Column column, String columnName, ColumnData columnData)
    {
        assert (column != null
                && columnName == null)
                || (column == null
                        && columnName != null) : "column and columnName are mutual exclusive";
        Column.MetaData metaData = columnData.metadata;
        ResolvedType type = columnData.resolvedType;
        CoreColumn.Type columnType = columnData.columnType;
        TableSourceReference tableSourceRef = columnData.tableSourceReference;
        if (tableSourceRef == null
                && column != null)
        {
            tableSourceRef = SchemaUtils.getTableSource(column);
        }

        ColumnReference cr = null;
        if (tableSourceRef != null)
        {
            cr = new ColumnReference(column != null ? column.getName()
                    : columnName, tableSourceRef, metaData);
        }

        CoreColumn.Builder builder = column != null ? CoreColumn.Builder.from(column)
                : CoreColumn.Builder.from(columnName, type);
        return builder.withColumnType(columnType)
                .withMetaData(metaData)
                .withResolvedType(type)
                .withColumnReference(cr)
                .build();
    }

    private static List<ColumnData> getColumnDataFromRows(TableSourceReference parentTableSourceReference, List<List<IExpression>> rowsExpressions, Location location)
    {
        if (rowsExpressions.isEmpty())
        {
            return emptyList();
        }

        int count = rowsExpressions.get(0)
                .size();
        List<ColumnData> columnDatas = new ArrayList<>(count);
        int rowCount = rowsExpressions.size();
        for (int i = 0; i < count; i++)
        {
            ResolvedType resolvedType = ResolvedType.ANY;
            TableSourceReference tableSourceReference = null;
            CoreColumn.Type columnType = CoreColumn.Type.REGULAR;
            Column.MetaData metaData = Column.MetaData.EMPTY;

            for (int j = 0; j < rowCount; j++)
            {
                List<IExpression> rowExpressions = rowsExpressions.get(j);

                if (rowExpressions.size() != count)
                {
                    throw new ParseException("All rows expressions must be of equal size.", location);
                }

                // Highest precedence type from each column
                IExpression expression = rowExpressions.get(i);
                if (expression.getType()
                        .getType()
                        .getPrecedence() > resolvedType.getType()
                                .getPrecedence())
                {
                    resolvedType = expression.getType();
                }

                columnType = SchemaUtils.getColumnType(rowExpressions.get(i));
                ColumnReference cr = SchemaUtils.getColumnReference(rowExpressions.get(i));
                if (cr == null)
                {
                    continue;
                }

                if (tableSourceReference == null)
                {
                    tableSourceReference = cr.tableSourceReference();
                    metaData = cr.metaData();
                }
                // Different table sources among column => clear
                else if (tableSourceReference.getId() != cr.tableSourceReference()
                        .getId())
                {
                    tableSourceReference = null;
                    metaData = Column.MetaData.EMPTY;
                    columnType = CoreColumn.Type.REGULAR;
                    break;
                }
            }

            if (tableSourceReference == null)
            {
                tableSourceReference = parentTableSourceReference;
                metaData = Column.MetaData.EMPTY;
            }

            columnDatas.add(new ColumnData(resolvedType, tableSourceReference, columnType, metaData));
        }
        return columnDatas;
    }

    private record ColumnData(ResolvedType resolvedType, TableSourceReference tableSourceReference, CoreColumn.Type columnType, Column.MetaData metadata)
    {
    }
}
