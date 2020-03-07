package com.viskan.payloadbuilder.codegen;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.janino.ClassBodyEvaluator;

/** Code generator */
public class CodeGenerator
{
//    private static final String BI_PREDICATE = System.lineSeparator() +
//        "public boolean test(Object __a, Object __b) " + System.lineSeparator() +
//        "{ " + System.lineSeparator() +
//        "  Row r_out = (Row) __a;" + System.lineSeparator() +
//        "  Row r_in = (Row) __b;" + System.lineSeparator() +
//        "  %s" + System.lineSeparator() +
//        "  return !%s && %s;" + System.lineSeparator() +
//        "}";

    private static final String PREDICATE = System.lineSeparator() +
        "public boolean test(Object __row) " + System.lineSeparator() +
        "{ " + System.lineSeparator() +
        "  final Row row = (Row) __row;" + System.lineSeparator() +
        "  %s" + System.lineSeparator() +
        "  return !%s && %s;" + System.lineSeparator() +
        "}";

    private static final String FUNCTION = System.lineSeparator() +
        "public Object apply(Object __row) " + System.lineSeparator() +
        "{ " + System.lineSeparator() +
        "  final Row row = (Row) __row;" + System.lineSeparator() +
        "  %s" + System.lineSeparator() +
        "  return %s ? null : %s;" + System.lineSeparator() +
        "}";

//    /** Generate a bit predicate. Used in joins */
//    public BaseBiPredicate generateBiPredicate(TableAlias tableAlias, Expression expression)
//    {
//        ExpressionCode code = generate(expression, tableAlias, true);
//        String generatedCode = String.format(BI_PREDICATE, code.code, code.isNull, code.resVar);
//        return compile(generatedCode, BaseBiPredicate.class, "BiPredicate");
//    }

    /** Generate a predicate. Used filters */
    public BasePredicate generatePredicate(TableAlias tableAlias, Expression expression)
    {
        ExpressionCode code = generate(expression, tableAlias);
        String generatedCode = String.format(PREDICATE, code.getCode(), code.getIsNull(), code.getResVar());
        return compile(code.getImports(), generatedCode, BasePredicate.class, "Predicate");
    }

    /** Generate a function. Used in expression projections etc. */
    public BaseFunction generateFunction(TableAlias tableAlias, Expression expression)
    {
        ExpressionCode code = generate(expression, tableAlias);
        String generatedCode = String.format(FUNCTION, code.getCode(), code.getIsNull(), code.getResVar());
        return compile(code.getImports(), generatedCode, BaseFunction.class, "Function");
    }
    
    /** Generate code for expression */
    private ExpressionCode generate(Expression expression, TableAlias tableAlias)
    {
        CodeGeneratorContext context = new CodeGeneratorContext();
//        context.biPredicate = biPredicate;
        context.tableAlias = tableAlias;
//        context.pretty = false;
        return expression.generateCode(context, null);
    }


    @SuppressWarnings("unchecked")
    private <T> T compile(Set<String> imports, String code, Class<T> baseClass, String name)
    {
        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setClassName("com.viskan.payloaduilder.codegen.Generated" + name);
        cbe.setExtendedClass(baseClass);
        
        List<String> usedImports = new ArrayList<>();
        usedImports.addAll(asList(
                "com.viskan.payloadbuilder.Row",
                "com.viskan.payloadbuilder.evaluation.ExpressionMath"
                ));
        usedImports.addAll(imports);
        
        cbe.setDefaultImports(usedImports.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
        
//        cbe.setDefaultImports(
//                new String[] {
//                        "com.viskan.payloadbuilder.Row",
//                        "com.viskan.payloadbuilder.evaluation.ExpressionMath"
//                        "com.viskan.payloadbuilder.utils.*",
//                });

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
