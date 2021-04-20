package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Returns first item if not null else second item */
class IsNullFunction extends ScalarFunctionInfo
{
    IsNullFunction(Catalog catalog)
    {
        super(catalog, "isnull");
    }

    @Override
    public String getDescription()
    {
        return "Returns first non null value of provided arguments. " + System.lineSeparator()
            + "Ex. isnull(expression, expression)" + System.lineSeparator()
            + "If both arguments yield null, null is returned.";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        if (obj != null)
        {
            return obj;
        }

        return arguments.get(1).eval(context);
    }

    @Override
    public DataType getDataType(List<Expression> arguments)
    {
        DataType aType = arguments.get(0).getDataType();
        DataType bType = arguments.get(1).getDataType();
        if (aType == bType)
        {
            return aType;
        }
        return DataType.ANY;
    }

    @Override
    public boolean isCodeGenSupported(List<Expression> arguments)
    {
        return arguments.get(0).isCodeGenSupported() && arguments.get(1).isCodeGenSupported();
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, List<Expression> arguments)
    {
        ExpressionCode code = context.getExpressionCode();

        ExpressionCode arg0Code = arguments.get(0).generateCode(context);
        ExpressionCode arg1Code = arguments.get(1).generateCode(context);

        /*
         * Object v_0 = ...
         * boolean n_0 = ....
         *
         * Object v_2 = v_0;
         * boolean n_2 = n_0;
         * if (n_0)
         * {
         *   Object v_1 = ...
         *   boolean n_1 = ....
         *
         *   v_2 = v_1;
         *   n_0 = n_1;
         * }
         */

        String template = "// isnull \n"
            + "%s"                              // arg0 code
            + "boolean %s = %s;\n"              // nullVar, arg0 nullVar
            + "%s %s = %s;\n"                   // datatype, resVar, arg0 resVar
            + "if (%s)\n"                       // arg0 nullVar
            + "{\n"
            + "  %s"                            // arg1 code
            + "  %s = %s;\n"                    // nullVar, arg1 nullVar
            + "  %s = %s;\n"                    // resVar, arg1 resVar
            + "}\n";
        code.setCode(String.format(template,
                arg0Code.getCode(),
                code.getNullVar(), arg0Code.getNullVar(),
                getDataType(arguments).getJavaTypeString(), code.getResVar(), arg0Code.getResVar(),
                arg0Code.getNullVar(),
                arg1Code.getCode(),
                code.getNullVar(), arg1Code.getNullVar(),
                code.getResVar(), arg1Code.getResVar()));
        return code;
    }
}
