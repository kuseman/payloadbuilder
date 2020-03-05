package com.viskan.payloadbuilder.codegen;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.parser.tree.Expression;

import org.codehaus.janino.ClassBodyEvaluator;

/** Code generator */
public class CodeGenerator
{
    private static final String BI_PREDICATE = System.lineSeparator() +
        "public boolean test(Object __a, Object __b) " + System.lineSeparator() +
        "{ " + System.lineSeparator() +
        "  Row r_out = (Row) __a;" + System.lineSeparator() +
        "  Row r_in = (Row) __b;" + System.lineSeparator() +
        "  %s" + System.lineSeparator() +
        "  return !%s && %s;" + System.lineSeparator() +
        "}";

    private static final String PREDICATE = System.lineSeparator() +
        "public boolean test(Object __a) " + System.lineSeparator() +
        "{ " + System.lineSeparator() +
        "  Row row = (Row) __a;" + System.lineSeparator() +
        "  %s" + System.lineSeparator() +
        "  return !%s && %s;" + System.lineSeparator() +
        "}";

    private static final String FUNCTION = System.lineSeparator() +
        "public Object apply(Object __a) " + System.lineSeparator() +
        "{ " + System.lineSeparator() +
        "  Row row = (Row) __a;" + System.lineSeparator() +
        "  %s" + System.lineSeparator() +
        "  return %s ? null : %s;" + System.lineSeparator() +
        "}";

    /** Generate a bit predicate. Used in joins */
    public BaseBiPredicate generateBiPredicate(TableAlias tableAlias, Expression expression)
    {
        ExpressionCode code = generate(expression, tableAlias, true);
        String generatedCode = String.format(BI_PREDICATE, code.code, code.isNull, code.resVar);
        return compile(generatedCode, BaseBiPredicate.class, "BiPredicate");
    }

    /** Generate a predicate. Used filters */
    public BasePredicate generatePredicate(TableAlias tableAlias, Expression expression)
    {
        ExpressionCode code = generate(expression, tableAlias, false);
        String generatedCode = String.format(PREDICATE, code.code, code.isNull, code.resVar);
        return compile(generatedCode, BasePredicate.class, "Predicate");
    }

    /** Generate a function. Used in expression projections etc. */
    public BaseFunction generateFunction(TableAlias tableAlias, Expression expression)
    {
        ExpressionCode code = generate(expression, tableAlias, false);
        String generatedCode = String.format(FUNCTION, code.code, code.isNull, code.resVar);
        return compile(generatedCode, BaseFunction.class, "Function");
    }
    
    /** Generate code for expression */
    private ExpressionCode generate(Expression expression, TableAlias tableAlias, boolean biPredicate)
    {
        CodeGeneratorContext context = new CodeGeneratorContext();
//        context.biPredicate = biPredicate;
        context.tableAlias = tableAlias;
//        context.pretty = false;
        return expression.generateCode(context, null);
    }


    @SuppressWarnings("unchecked")
    private <T> T compile(String code, Class<T> baseClass, String name)
    {
        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setClassName("com.viskan.payloaduilder.codegen.Generated" + name);
        cbe.setExtendedClass(baseClass);
        cbe.setDefaultImports(
                new String[] {
                        "com.viskan.payloadbuilder.Row",
                        "com.viskan.payloadbuilder.utils.*",
                        "com.viskan.payloadbuilder.evaluation.ExpressionMath"
                });

        try
        {
            cbe.cook(code);
            Class<?> clazz = cbe.getClazz();
            return (T) clazz.newInstance();
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Error compiling code: " + code, e);
        }

    }
}
