package org.kuse.payloadbuilder.core.catalog;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.NotImplementedException;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Definition of a scalar function */
//CSOFF
public abstract class ScalarFunctionInfo extends FunctionInfo
//CSON
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
     * <b>aa.filter(aa, x -> x.sku_id > 0)</b>
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

    /** Evaluate this function */
    @SuppressWarnings("unused")
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        throw new NotImplementedException("eval: " + getClass().getSimpleName());
    }

    /**
     * Returns true if this function supports code generation
     *
     * @param arguments Function arguments
     */
    public boolean isCodeGenSupported(List<Expression> arguments)
    {
        return false;
    }

    /** Returns true if this function is contant */
    public boolean isConstant(List<Expression> arguments)
    {
        return arguments.stream().allMatch(e -> e.isConstant());
    }

    /**
     * Generate code for this function. Default is fallback to eval.
     *
     * @param context Context used during evaluation
     * @param arguments Arguments to function
     **/
    public ExpressionCode generateCode(CodeGeneratorContext context, List<Expression> arguments)
    {
        context.addImport("org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo");
        context.addImport("java.util.List");

        int index = context.addReference(this);
        int index2 = context.addReference(arguments);
        ExpressionCode code = context.getExpressionCode();
        code.setCode(String.format(
                "boolean %s = true;\n"                                                                               // nullVar
                    + "Object %s = ((ScalarFunctionInfo) references[%d]).eval(context, ((List) references[%d]));\n"  // resVar, index, index2
                    + "%s = %s == null;\n",                                                                          // nullVar, resVar
                code.getNullVar(),
                code.getResVar(), index, index2,
                code.getNullVar(), code.getResVar()));

        return code;
    }
}
