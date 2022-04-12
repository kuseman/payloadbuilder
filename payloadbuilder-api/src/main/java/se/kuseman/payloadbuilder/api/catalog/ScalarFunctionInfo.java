package se.kuseman.payloadbuilder.api.catalog;

import java.util.List;
import java.util.Set;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Definition of a scalar function */
// CSOFF
public abstract class ScalarFunctionInfo extends FunctionInfo
// CSON
{
    public ScalarFunctionInfo(Catalog catalog, String name)
    {
        super(catalog, name, Type.SCALAR);
    }

    /**
     * Resolves resulting aliases for this function for provided parent aliases.
     *
     * <pre>
     * Example:
     * <b>unionall(aa, aa.ap)</b>
     * This has s (source) as parent alias and will resolve both arguments as resulting aliases
     * [aa, ap]
     *
     * Example:
     * <b>aa.filter(aa, x -&gt; x.sku_id &gt; 0)</b>
     * Resulting alias of a filter is the first arguments resulting alias ie. [aa]
     * </pre>
     *
     * @param parentAliases Parent aliases in context to this function
     * @param argumentAliases Resulting aliases for earch function argument
     **/
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        return parentAliases;
    }

    /**
     * Data type of this function
     *
     * @param arguments Supplier for arguments data types
     */
    public DataType getDataType(List<? extends IExpression> arguments)
    {
        return DataType.ANY;
    }

    /** Evaluate this function */
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        throw new IllegalArgumentException("Not implemented. eval: " + getClass().getSimpleName());
    }

    /**
     * Returns true if this function supports code generation
     *
     * @param arguments Function arguments
     */
    public boolean isCodeGenSupported(List<? extends IExpression> arguments)
    {
        return false;
    }

    /** Returns true if this function is contant */
    public boolean isConstant(List<? extends IExpression> arguments)
    {
        return arguments.stream()
                .allMatch(e -> e.isConstant());
    }

    /**
     * Generate code for this function. Default is fallback to eval.
     *
     * @param context Context used during evaluation
     * 
     * @param arguments Arguments to function
     **/
    public ExpressionCode generateCode(CodeGeneratorContext context, List<? extends IExpression> arguments)
    {
        context.addImport("se.kuseman.payloadbuilder.core.catalog.ScalarFunctionInfo");
        context.addImport("java.util.List");

        int index = context.addReference(this);
        int index2 = context.addReference(arguments);
        ExpressionCode code = context.getExpressionCode();
        code.setCode(String.format("boolean %s = true;\n" // nullVar
                                   + "Object %s = ((ScalarFunctionInfo) references[%d]).eval(context, ((List) references[%d]));\n" // resVar, index, index2
                                   + "%s = %s == null;\n", // nullVar, resVar
                code.getNullVar(), code.getResVar(), index, index2, code.getNullVar(), code.getResVar()));

        return code;
    }
}
