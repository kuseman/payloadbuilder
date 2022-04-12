package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.TableMeta;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.utils.ObjectUtils;

/** Function listOf. Creates a list of provided arguments */
class ContainsFunction extends ScalarFunctionInfo
{
    ContainsFunction(Catalog catalog)
    {
        super(catalog, "contains");
    }

    @Override
    public String getDescription()
    {
        return "Checks if provided collection contains value argument" + System.lineSeparator() + "ie. contains(<collection expression>, <value expression>)";
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public TableMeta.DataType getDataType(List<? extends IExpression> arguments)
    {
        return TableMeta.DataType.BOOLEAN;
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        // Function has a declared getInputTypes method that guards against wrong argument count
        Object arg0 = arguments.get(0)
                .eval(context);
        Object arg1 = arguments.get(1)
                .eval(context);

        return ObjectUtils.contains(arg0, arg1);
    }

    @Override
    public boolean isCodeGenSupported(List<? extends IExpression> arguments)
    {
        return arguments.stream()
                .allMatch(IExpression::isCodeGenSupported);
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, List<? extends IExpression> arguments)
    {
        ExpressionCode code = context.getExpressionCode();
        context.addImport("se.kuseman.payloadbuilder.core.utils.ObjectUtils");

        ExpressionCode arg0Code = arguments.get(0)
                .generateCode(context);
        ExpressionCode arg1Code = arguments.get(1)
                .generateCode(context);

        String template = "%s%s" // arg0Code, arg1Code
                          + "boolean %s = false;\n" // nullVar
                          + "boolean %s = ObjectUtils.contains(%s, %s);\n"; // arg0 resVar, org1 resVar
        code.setCode(String.format(template, arg0Code.getCode(), arg1Code.getCode(), code.getNullVar(), code.getResVar(), arg0Code.getResVar(), arg1Code.getResVar()));
        return code;
    }
}
