package se.kuseman.payloadbuilder.catalog.fs;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** File function */
class FileFunction extends TableFunctionInfo
{
    //@formatter:off
    static final Schema SCHEMA = Schema.of(
            Column.of("name", ResolvedType.of(Type.String)),
            Column.of("path", ResolvedType.of(Type.String)),
            Column.of("size", ResolvedType.of(Type.Long)),
            Column.of("exists", ResolvedType.of(Type.Boolean)),
            Column.of("creationTime", ResolvedType.of(Type.DateTime)),
            Column.of("lastAccessTime", ResolvedType.of(Type.DateTime)),
            Column.of("lastModifiedTime", ResolvedType.of(Type.DateTime)),
            Column.of("isDirectory", ResolvedType.of(Type.Boolean)));
    //@formatter:on

    FileFunction()
    {
        super("file");
    }

    @Override
    public Schema getSchema(List<IExpression> arguments)
    {
        return SCHEMA;
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
    {
        ValueVector value = arguments.get(0)
                .eval(context);
        if (value.isNull(0))
        {
            return TupleIterator.EMPTY;
        }
        String strPath = value.valueAsString(0);
        Path path = FileSystems.getDefault()
                .getPath(strPath);
        Object[] values = ListFunction.fromPath(path);
        return TupleIterator.singleton(new ObjectTupleVector(SCHEMA, 1, (row, col) -> values[col]));
    }
}