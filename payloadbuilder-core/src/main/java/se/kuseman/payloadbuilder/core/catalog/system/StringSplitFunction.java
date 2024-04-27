package se.kuseman.payloadbuilder.core.catalog.system;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Table valued function that splits a string and returns a table from the splitted result */
class StringSplitFunction extends TableFunctionInfo
{
    private static final Schema SCHEMA = Schema.of(Column.of("Value", ResolvedType.of(Type.String)), Column.of("Ordinal", ResolvedType.of(Type.Int)));

    StringSplitFunction()
    {
        super("string_split");
    }

    @Override
    public Schema getSchema(List<IExpression> arguments)
    {
        return SCHEMA;
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, IDatasourceOptions options)
    {
        final ValueVector value = arguments.get(0)
                .eval(context);
        final ValueVector separator = arguments.get(1)
                .eval(context);

        if (value.isNull(0)
                || separator.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        String strValue = value.getString(0)
                .toString();

        if (isBlank(strValue))
        {
            return TupleIterator.EMPTY;
        }

        String strSeparator = separator.getString(0)
                .toString();
        String[] parts = StringUtils.split(strValue, strSeparator);

        int length = parts.length;
        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Type.String), length);
        for (int i = 0; i < length; i++)
        {
            resultVector.setString(i, UTF8String.from(parts[i]));
        }

        return TupleIterator.singleton(TupleVector.of(schema.get(), List.of(resultVector, ValueVector.range(0, length))));
    }
}
