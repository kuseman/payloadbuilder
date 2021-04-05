package org.kuse.payloadbuilder.core.codegen;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.ArrayUtils;
import org.codehaus.janino.ClassBodyEvaluator;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/**
 * Code generator Concept inspired by Apache Spark
 * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/CodeGenerator.scala
 **/
public class CodeGenerator
{
    private static final String PREDICATE = System.lineSeparator()
        + "public boolean test(Object __ctx) " + System.lineSeparator()
        + "{ " + System.lineSeparator()
        + "  final Tuple tuple = ((ExecutionContext) __ctx).getTuple();" + System.lineSeparator()
        + "  %s" + System.lineSeparator()
        + "  return %s != null && (Boolean) %s;" + System.lineSeparator()
        + "}";

    private static final String FUNCTION = System.lineSeparator()
        + "public Object apply(Object __ctx) " + System.lineSeparator()
        + "{ " + System.lineSeparator()
        + "  final Tuple tuple = ((ExecutionContext) __ctx).getTuple();" + System.lineSeparator()
        + "  %s" + System.lineSeparator()
        + "  return %s;" + System.lineSeparator()
        + "}";

    /** Generate a predicate. Used filters */
    public Predicate<ExecutionContext> generatePredicate(Expression expression)
    {
        CodeGeneratorContext context = new CodeGeneratorContext();
        ExpressionCode code = expression.generateCode(context);
        String generatedCode = String.format(PREDICATE, code.getCode(), code.getResVar(), code.getResVar());
        BasePredicate predicate = compile(context.getImports(), generatedCode, BasePredicate.class, "Predicate");
        predicate.setExpression(expression);
        return predicate;
    }

    /** Generate a function. Used in expression projections etc. */
    public Function<ExecutionContext, Object> generateFunction(Expression expression)
    {
        CodeGeneratorContext context = new CodeGeneratorContext();
        ExpressionCode code = expression.generateCode(context);
        String generatedCode = String.format(FUNCTION, code.getCode(), code.getResVar());
        BaseFunction function = compile(context.getImports(), generatedCode, BaseFunction.class, "Function");
        function.setExpression(expression);
        return function;
    }

    @SuppressWarnings("unchecked")
    private <T> T compile(Set<String> imports, String code, Class<T> baseClass, String name)
    {
        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setClassName("org.kuse.payloaduilder.codegen.Generated" + name);
        cbe.setExtendedClass(baseClass);

        List<String> usedImports = new ArrayList<>();
        usedImports.addAll(asList(
                "org.kuse.payloadbuilder.core.operator.Tuple",
                "org.kuse.payloadbuilder.core.utils.ExpressionMath",
                "org.kuse.payloadbuilder.core.operator.ExecutionContext"));
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
