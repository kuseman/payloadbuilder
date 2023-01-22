package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Concat function. Concatenates all arguments into a string */
class ConcatFunction extends ScalarFunctionInfo
{
    ConcatFunction(Catalog catalog)
    {
        super(catalog, "concat", FunctionType.SCALAR);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final int expressionSize = arguments.size();
        final ValueVector[] vectors = new ValueVector[expressionSize];
        for (int i = 0; i < expressionSize; i++)
        {
            vectors[i] = arguments.get(i)
                    .eval(input, context);
        }
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
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public UTF8String getString(int row)
            {
                List<UTF8String> strings = null;
                for (int i = 0; i < expressionSize; i++)
                {
                    // Null values are ignored
                    if (vectors[i].isNull(row))
                    {
                        continue;
                    }

                    if (strings == null)
                    {
                        strings = singletonList(vectors[i].getString(row));
                        continue;
                    }
                    else if (strings.size() == 1)
                    {
                        UTF8String tmp = strings.get(0);
                        strings = new ArrayList<>(5);
                        strings.add(tmp);
                    }

                    strings.add(vectors[i].getString(row));
                }

                return strings == null ? UTF8String.EMPTY
                        : UTF8String.concat(UTF8String.EMPTY, strings);
            }

            @Override
            public Object getValue(int row)
            {
                return getString(row);
            }
        };
    }

    // @Override
    // public Object eval(IExecutionContext ctx, String catalogAlias, List<? extends IExpression> arguments)
    // {
    // int size = arguments.size();
    // if (size <= 0)
    // {
    // return null;
    // }
    //
    // ExecutionContext context = (ExecutionContext) ctx;
    // StringBuilder sb = new StringBuilder();
    // for (IExpression arg : arguments)
    // {
    // Object object = arg.eval(context);
    // if (object != null)
    // {
    // sb.append(EvalUtils.unwrap(context, object));
    // }
    // }
    // return sb.toString();
    // }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    // @Override
    // public ExpressionCode generateCode(
    // CodeGeneratorContext context,
    // ExpressionCode parentCode,
    // List<Expression> arguments)
    // {
    // ExpressionCode code = ExpressionCode.code(context, parentCode);
    // context.addImport("se.kuseman.payloadbuilder.core.utils.ObjectUtils");
    //
    // List<String> argsResVars = new ArrayList<>(arguments.size());
    // StringBuilder sb = new StringBuilder();
    // for (Expression arg : arguments)
    // {
    // ExpressionCode argCode = arg.generateCode(context, parentCode);
    // argsResVars.add(argCode.getResVar());
    // sb.append(argCode.getCode());
    // }
    //
    // // TODO: Fix iterator concating even if arguments are object
    //
    // String template = "%s"
    // + "Object %s = ObjectUtils.concat(%s);\n";
    //
    // code.setCode(String.format(template,
    // sb.toString(),
    // code.getIsNull(),
    // code.getResVar(),
    // argsResVars.stream().collect(joining(","))));
    // return code;
    // }
}
