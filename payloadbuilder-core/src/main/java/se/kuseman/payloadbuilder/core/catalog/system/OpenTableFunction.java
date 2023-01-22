package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.Optional;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Table value function that opens a {@link Column.Type#Table} value from provided argument expression
 *
 * <pre>
 *
 * Example
 *
 * SELECT field
 * FROM source s
 * CROSS APPLY open_table(s.tableColumn) y
 * 
 * </pre>
 */
class OpenTableFunction extends TableFunctionInfo
{
    OpenTableFunction()
    {
        super("open_table");
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public Schema getSchema(List<IExpression> arguments) throws SchemaResolveException
    {
        ResolvedType type = arguments.get(0)
                .getType();
        if (!(type.getType() == Column.Type.Any
                || type.getType() == Column.Type.Table))
        {
            throw new SchemaResolveException("Function " + getName() + " requires input to be of Any or Table type, got: " + type);
        }

        return type.getType() == Column.Type.Any ? Schema.EMPTY
                : type.getSchema();
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, IDatasourceOptions options)
    {
        final ValueVector vector = arguments.get(0)
                .eval(context);

        if (vector.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        final TupleVector table = vector.getTable(0);
        return TupleIterator.singleton(new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema.orElse(table.getSchema());
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
}
