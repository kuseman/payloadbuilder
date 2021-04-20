package org.kuse.payloadbuilder.core.codegen;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntBiFunction;

import org.apache.commons.lang3.ArrayUtils;
import org.codehaus.janino.ClassBodyEvaluator;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.IIndexValuesFactory;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.RootProjection;
import org.kuse.payloadbuilder.core.operator.Tuple;
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
        + "  final StatementContext context = ((ExecutionContext) __ctx).getStatementContext();" + System.lineSeparator()
        + "  final Tuple tuple = context.getTuple();" + System.lineSeparator()
        + "  %s" + System.lineSeparator()
        + "  return !%s && %s%s;" + System.lineSeparator()
        + "}";

    private static final String FUNCTION = System.lineSeparator()
        + "public Object apply(Object __ctx) " + System.lineSeparator()
        + "{ " + System.lineSeparator()
        + "  final StatementContext context = ((ExecutionContext) __ctx).getStatementContext();" + System.lineSeparator()
        + "  final Tuple tuple = context.getTuple();" + System.lineSeparator()
        + "  %s" + System.lineSeparator()
        + "  return %s;" + System.lineSeparator()
        + "}";

    private static final String HASH_FUNCTION = System.lineSeparator()
        + "public int applyAsInt(Object __ctx, Object __tuple) " + System.lineSeparator()
        + "{ " + System.lineSeparator()
        + "  final StatementContext context = ((ExecutionContext) __ctx).getStatementContext();" + System.lineSeparator()
        + "  final Tuple tuple = (Tuple) __tuple;" + System.lineSeparator()
        + "  context.setTuple(tuple);" + System.lineSeparator()
        + "  %s" + System.lineSeparator()
        + "  context.setTuple(null);" + System.lineSeparator()
        + "  return %s;" + System.lineSeparator()
        + "}";

    private static final String PROECTION = System.lineSeparator()
        + "public void writeValue(final OutputWriter writer, final ExecutionContext context)" + System.lineSeparator()
        + "{ " + System.lineSeparator()
        + "  %s" + System.lineSeparator()
        + "}";

    private static final String INDEX_VALUE_FACTORY = "\n"
        + "  public IIndexValues create(ExecutionContext context, Tuple tuple) \n"
        + "  { \n"
        + "    context.getStatementContext().setTuple(tuple); \n"
        + "    %s"
        + "    IndexValues res = new IndexValues(%s); \n"
        + "    context.getStatementContext().setTuple(null); \n"
        + "    return res; \n"
        + "  } \n"

        + "  private static class IndexValues extends BaseIndexValues\n"
        + "  {\n"
        + "    private final int hashCode; \n"
        + "    %s"
        + "    private IndexValues(%s) \n"
        + "    { \n"
        + "      %s"
        + "      this.hashCode = hash(); \n"
        + "    } \n"
        + "\n"
        + "    public int size() \n"
        + "    { \n"
        + "      return %d; \n"
        + "    } \n"
        + "\n"
        + "    public DataType getType(int ordinal) \n"
        + "    { \n"
        + "      %s"
        + "      throw new IllegalArgumentException(\"Invalid ordinal\"); \n"
        + "    } \n"
        + "\n"
        + "    public Object getValue(int ordinal) \n"
        + "    { \n"
        + "      %s"
        + "      throw new IllegalArgumentException(\"Invalid ordinal\"); \n"
        + "    } \n"
        + "\n"
        + "    public int getInt(int ordinal) \n"
        + "    { \n"
        + "      %s"
        + "      throw new IllegalArgumentException(\"Invalid ordinal\"); \n"
        + "    } \n"
        + "\n"
        + "    public long getLong(int ordinal) \n"
        + "    { \n"
        + "      %s"
        + "      throw new IllegalArgumentException(\"Invalid ordinal\"); \n"
        + "    } \n"
        + "\n"
        + "    public float getFloat(int ordinal) \n"
        + "    { \n"
        + "      %s"
        + "      throw new IllegalArgumentException(\"Invalid ordinal\"); \n"
        + "    } \n"
        + "\n"
        + "    public double getDouble(int ordinal) \n"
        + "    { \n"
        + "      %s"
        + "      throw new IllegalArgumentException(\"Invalid ordinal\"); \n"
        + "    } \n"
        + "\n"
        + "    public boolean getBool(int ordinal) \n"
        + "    { \n"
        + "      %s"
        + "      throw new IllegalArgumentException(\"Invalid ordinal\"); \n"
        + "    } \n"
        + "\n"
        + "    public int hashCode() \n"
        + "    { \n"
        + "      return hashCode; \n"
        + "    } \n"
        + "\n"
        + "    private int hash() \n"
        + "    { \n"
        + "      int hash = 17; \n"
        + "      %s"
        + "      return hash; \n"
        + "    } \n"
        + "  } \n";

    /** Generate a predicate. Used filters */
    public Predicate<ExecutionContext> generatePredicate(Expression expression)
    {
        CodeGeneratorContext context = new CodeGeneratorContext();
        ExpressionCode code = expression.generateCode(context);
        String generatedCode = String.format(PREDICATE, code.getCode(), code.getNullVar(), expression.getDataType() != DataType.BOOLEAN ? "(Boolean)" : "", code.getResVar());
        BasePredicate predicate = compile(context.getImports(), generatedCode, BasePredicate.class, "Predicate");
        predicate.references = context.references.toArray();
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
        function.references = context.references.toArray();
        function.setExpression(expression);
        return function;
    }

    /** Generate a has function. Used in joins etc. */
    public ToIntBiFunction<ExecutionContext, Tuple> generateHashFunction(List<Expression> expressions)
    {
        CodeGeneratorContext context = new CodeGeneratorContext();
        context.addImport("org.kuse.payloadbuilder.core.utils.ObjectUtils");
        String resVar = context.newVar("r");
        StringBuilder sb = new StringBuilder("int " + resVar + " = 17;\n");
        for (Expression expression : expressions)
        {
            ExpressionCode code = expression.generateCode(context);
            sb.append(code.getCode());
            sb.append(resVar + " = " + resVar + " * 37 + (" + code.getNullVar() + " ? 0 : ObjectUtils.hash(" + code.getResVar() + "));").append(System.lineSeparator());
        }
        String generatedCode = String.format(HASH_FUNCTION, sb, resVar);
        BaseHashFunction hashFunction = compile(context.getImports(), generatedCode, BaseHashFunction.class, "HashFunction");
        hashFunction.references = context.references.toArray();
        hashFunction.setExpressions(expressions);
        return hashFunction;
    }

    /** Generate code for provided root projection */
    public Projection generateProjection(RootProjection rootProjection)
    {
        CodeGeneratorContext context = new CodeGeneratorContext();
        context.addImport("org.kuse.payloadbuilder.core.OutputWriter");
        context.addImport("org.kuse.payloadbuilder.core.operator.Projection");
        ProjectionCode code = rootProjection.generateCode(context);
        String generatedCode = String.format(PROECTION, code.getCode());
        BaseProjection projection = compile(context.getImports(), generatedCode, BaseProjection.class, "Projection");
        projection.setProjection(rootProjection);
        projection.references = context.references.toArray();
        return projection;
    }

    /** Generate a {@link IIndexValuesFactory} from provide expressions */
    //CSOFF
    public IIndexValuesFactory generateIndexValuesFactory(List<Expression> expressions)
    //CSON
    {
        CodeGeneratorContext context = new CodeGeneratorContext();
        context.addImport("java.util.Objects");
        context.addImport("org.kuse.payloadbuilder.core.codegen.BaseGeneratedClass");
        context.addImport("org.kuse.payloadbuilder.core.operator.IIndexValuesFactory");
        context.addImport("org.kuse.payloadbuilder.core.utils.ObjectUtils");
        context.addImport("org.kuse.payloadbuilder.core.catalog.TableMeta.DataType");

        StringBuilder expressionCode = new StringBuilder();
        StringBuilder argFields = new StringBuilder();
        StringBuilder fields = new StringBuilder();
        StringBuilder ctorArgs = new StringBuilder();
        StringBuilder ctorFiels = new StringBuilder();
        StringBuilder getType = new StringBuilder();
        StringBuilder getValue = new StringBuilder();
        StringBuilder getInt = new StringBuilder();
        StringBuilder getLong = new StringBuilder();
        StringBuilder getFloat = new StringBuilder();
        StringBuilder getDouble = new StringBuilder();
        StringBuilder getBool = new StringBuilder();
        StringBuilder hashCode = new StringBuilder();
        int size = expressions.size();
        for (int i = 0; i < size; i++)
        {
            Expression expression = expressions.get(i);
            ExpressionCode code = expression.generateCode(context);

            expressionCode.append(code.getCode());
            // TODO: verify is null is allowed here (batch join, group by)
            expressionCode.append(String.format("if (%s) throw new IllegalArgumentException(\"%s yielded null\");", code.getNullVar(), expression.toString())).append(System.lineSeparator());

            // f_0,f_1,....
            if (i > 0)
            {
                argFields.append(",");
            }
            argFields.append(code.getResVar());

            DataType type = expression.getDataType();
            String typeName = type.getJavaTypeString();
            // private final <datatype> f_X;
            fields.append("private final ").append(typeName).append(" f_").append(i).append(";").append(System.lineSeparator());

            // int f_0, float f_1, ...
            if (i > 0)
            {
                ctorArgs.append(",");
            }
            ctorArgs.append(typeName).append(" f_").append(i);
            if (i != size - 1)
            {
                ctorArgs.append(System.lineSeparator());
            }

            // this.f_X = f_X;
            ctorFiels.append("this.f_").append(i).append(" = f_").append(i).append(";").append(System.lineSeparator());

            // if (ordinal == X) return <type>
            getType.append("if (ordinal == ").append(i).append(") return ").append("DataType.").append(type.name()).append(";").append(System.lineSeparator());

            // if (ordinal == X) return f_X;
            StringBuilder getBuilder = null;
            if ("Object".equals(typeName))
            {
                getBuilder = getValue;
            }
            else if ("int".equals(typeName))
            {
                getBuilder = getInt;
            }
            else if ("long".equals(typeName))
            {
                getBuilder = getLong;
            }
            else if ("float".equals(typeName))
            {
                getBuilder = getFloat;
            }
            else if ("double".equals(typeName))
            {
                getBuilder = getDouble;
            }
            else if ("boolean".equals(typeName))
            {
                getBuilder = getBool;
            }
            else
            {
                throw new IllegalArgumentException("Unkown type " + typeName);
            }

            getBuilder.append("if (ordinal == ").append(i).append(") return f_").append(i).append(";").append(System.lineSeparator());

            // hash = hash * 37 + ObjectUtils.hash(f_X);
            hashCode.append("hash = hash * 37 + ");
            if (typeName.equals("Object"))
            {
                hashCode.append("(f_").append(i).append(" == null ? 0 : ObjectUtils.hash(f_").append(i).append("));").append(System.lineSeparator());
            }
            else
            {
                hashCode.append("ObjectUtils.hash(f_").append(i).append(");").append(System.lineSeparator());
            }
        }

        String generatedCode = String.format(INDEX_VALUE_FACTORY,
                expressionCode,
                argFields,
                fields,
                ctorArgs,
                ctorFiels,
                size,
                getType,
                getValue,
                getInt,
                getLong,
                getFloat,
                getDouble,
                getBool,
                hashCode);

        BaseIndexValueFactory factory = compile(context.getImports(), generatedCode, BaseIndexValueFactory.class, "IndexValuesFactory");
        factory.setExpressions(expressions);
        factory.references = context.references.toArray();
        return factory;
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
                "org.kuse.payloadbuilder.core.operator.ExecutionContext",
                "org.kuse.payloadbuilder.core.operator.StatementContext"));
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
