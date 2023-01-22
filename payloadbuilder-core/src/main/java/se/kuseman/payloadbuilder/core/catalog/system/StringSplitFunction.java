package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Table valued function that splits a string and returns a table from the splitted result */
class StringSplitFunction extends TableFunctionInfo
{
    private static final Schema SCHEMA = Schema.of(Column.of("Value", ResolvedType.of(Type.String)));

    StringSplitFunction(Catalog catalog)
    {
        super(catalog, "string_split");
    }

    @Override
    public Schema getSchema(List<? extends IExpression> arguments)
    {
        return SCHEMA;
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments, IDatasourceOptions options)
    {
        final ValueVector value = eval(context, arguments.get(0));
        final ValueVector separator = eval(context, arguments.get(1));

        if ((value.isNullable()
                && value.isNull(0))
                || (separator.isNullable()
                        && separator.isNull(0)))
        {
            return TupleIterator.EMPTY;
        }

        String strValue = value.getString(0)
                .toString();
        String strSeparator = separator.getString(0)
                .toString();
        final String[] parts = strValue.split(strSeparator);

        return TupleIterator.singleton(new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return SCHEMA;
            }

            @Override
            public int getRowCount()
            {
                return parts.length;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return new ValueVector()
                {
                    @Override
                    public ResolvedType type()
                    {
                        return ResolvedType.of(Type.String);
                    }

                    @Override
                    public int size()
                    {
                        return parts.length;
                    }

                    @Override
                    public boolean isNullable()
                    {
                        return false;
                    }

                    @Override
                    public boolean isNull(int row)
                    {
                        return false;
                    }

                    @Override
                    public UTF8String getString(int row)
                    {
                        return UTF8String.from(parts[row]);
                    }

                    @Override
                    public Object getValue(int row)
                    {
                        throw new IllegalArgumentException("getValue should not be called on typed vectors");
                    }
                };
            }
        });
    }
}
