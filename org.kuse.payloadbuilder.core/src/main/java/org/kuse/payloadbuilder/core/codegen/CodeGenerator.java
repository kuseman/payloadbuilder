package org.kuse.payloadbuilder.core.codegen;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.codehaus.janino.ClassBodyEvaluator;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;

/**
 * Code generator Concept inspired by Apache Spark
 * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/CodeGenerator.scala
 **/
public class CodeGenerator
{
    private static final String PREDICATE = System.lineSeparator()
        + "public boolean test(Object __row) " + System.lineSeparator()
        + "{ " + System.lineSeparator()
        + "  final Row row = (Row) __row;" + System.lineSeparator()
        + "  %s" + System.lineSeparator()
        + "  return !%s && %s;" + System.lineSeparator()
        + "}";

    private static final String FUNCTION = System.lineSeparator()
        + "public Object apply(Object __row) " + System.lineSeparator()
        + "{ " + System.lineSeparator()
        + "  final Row row = (Row) __row;" + System.lineSeparator()
        + "  %s" + System.lineSeparator()
        + "  return %s ? null : %s;" + System.lineSeparator()
        + "}";

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
        context.setTableAlias(tableAlias);
        return expression.generateCode(context, null);
    }

    @SuppressWarnings("unchecked")
    private <T> T compile(Set<String> imports, String code, Class<T> baseClass, String name)
    {
        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setClassName("org.kuse.payloaduilder.codegen.Generated" + name);
        cbe.setExtendedClass(baseClass);

        List<String> usedImports = new ArrayList<>();
        usedImports.addAll(asList(
                "org.kuse.payloadbuilder.operator.Row",
                "org.kuse.payloadbuilder.parser.ExpressionMath"));
        usedImports.addAll(imports);

        cbe.setDefaultImports(usedImports.toArray(ArrayUtils.EMPTY_STRING_ARRAY));

        try
        {
            cbe.cook(code);
            Class<?> clazz = cbe.getClazz();
            return (T) clazz.getConstructors()[0].newInstance();
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Error compiling code: " + code, e);
        }
    }
}
