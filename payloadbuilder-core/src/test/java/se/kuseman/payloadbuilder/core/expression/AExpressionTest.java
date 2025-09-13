package se.kuseman.payloadbuilder.core.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;
import se.kuseman.payloadbuilder.core.logicalplan.optimization.LogicalPlanOptimizer;
import se.kuseman.payloadbuilder.core.parser.QueryParser;

/** Base class for expression based tests */
public abstract class AExpressionTest extends Assert
{
    protected static final QueryParser PARSER = new QueryParser();

    protected IExpression e(String expression)
    {
        return PARSER.parseExpression(expression);
    }

    /** Return a simple unresolved column expression */
    protected UnresolvedColumnExpression uce(String... parts)
    {
        return new UnresolvedColumnExpression(QualifiedName.of(parts), -1, null);
    }

    /** Return a simple reflective column expression that resolve values runtime by name */
    protected ColumnExpression ce(String col)
    {
        return ColumnExpression.Builder.of(col, ResolvedType.of(Type.Any))
                .withColumn(col)
                .build();
    }

    /** Return a simple reflective column expression that resolve values runtime by name with type */
    protected ColumnExpression ce(String col, ResolvedType type)
    {
        return ColumnExpression.Builder.of(col, type)
                .withColumn(col)
                .build();
    }

    /** Return a simple column expression that resolves values by ordinal */
    protected ColumnExpression ce(String alias, int ordinal)
    {
        return ce(alias, ordinal, ResolvedType.of(Type.Any));
    }

    /** Return a simple column expression that resolves values by ordinal with type */
    protected ColumnExpression ce(String alias, int ordinal, ResolvedType type)
    {
        if (ordinal < 0)
        {
            throw new IllegalArgumentException("Ordinal must be grater of equal to zero");
        }
        return ColumnExpression.Builder.of(alias, type)
                .withOrdinal(ordinal)
                .build();
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String alias, TableSourceReference tr, int ordinal)
    {
        return cre(alias, tr, ordinal, ResolvedType.of(Type.Any));
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String alias, TableSourceReference tr, int ordinal, ResolvedType type)
    {
        return cre(alias, tr, ordinal, type, CoreColumn.Type.REGULAR);
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String alias, TableSourceReference tr, TableSourceReference ttr, int ordinal, ResolvedType type)
    {
        return cre(alias, tr, ttr, ordinal, type, CoreColumn.Type.REGULAR);
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String alias, TableSourceReference tr, int ordinal, ResolvedType type, CoreColumn.Type columnType)
    {
        return cre(alias, tr, null, ordinal, type, columnType);
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String alias, TableSourceReference tr, TableSourceReference ttr, int ordinal, ResolvedType type, CoreColumn.Type columnType)
    {
        if (ordinal < 0)
        {
            throw new IllegalArgumentException("Ordinal must be grater of equal to zero");
        }
        return ColumnExpression.Builder.of(alias, type)
                .withOrdinal(ordinal)
                .withColumnReference(new ColumnReference(tr, columnType, ttr))
                .build();
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String alias, TableSourceReference tr, String column, ResolvedType type)
    {
        return cre(alias, tr, column, type, CoreColumn.Type.REGULAR);
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String alias, TableSourceReference tr, String column, ResolvedType type, CoreColumn.Type columnType)
    {
        return ColumnExpression.Builder.of(alias, type)
                .withColumn(column)
                .withColumnReference(new ColumnReference(tr, columnType))
                .build();
    }

    /** Create an {@link ColumnExpression} with no ordinal */
    protected ColumnExpression cre(String column, TableSourceReference tr)
    {
        return cre(column, tr, (TableSourceReference) null, ResolvedType.of(Type.Any), CoreColumn.Type.REGULAR);
    }

    /** Create an {@link ColumnExpression} with no ordinal */
    protected ColumnExpression cre(String column, TableSourceReference tr, TableSourceReference ttr)
    {
        return cre(column, tr, ttr, ResolvedType.of(Type.Any), CoreColumn.Type.REGULAR);
    }

    /** Create an {@link ColumnExpression} with no ordinal */
    protected ColumnExpression cre(String column, TableSourceReference tr, CoreColumn.Type columnType)
    {
        return cre(column, tr, ResolvedType.of(Type.Any), columnType);
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String column, TableSourceReference tr, ResolvedType type)
    {
        return cre(column, tr, type, CoreColumn.Type.REGULAR);
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String column, TableSourceReference tr, ResolvedType type, CoreColumn.Type columnType)
    {
        return cre(column, tr, (TableSourceReference) null, type, columnType);
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String column, TableSourceReference tr, TableSourceReference ttr, ResolvedType type, CoreColumn.Type columnType)
    {
        return ColumnExpression.Builder.of(column, type)
                .withColumn(column)
                .withColumnReference(new ColumnReference(tr, columnType, ttr))
                .build();
    }

    /** Create an {@link ColumnExpression} with lambda id */
    protected ColumnExpression lce(String alias, int lambdaId)
    {
        if (lambdaId < 0)
        {
            throw new IllegalArgumentException("lambdaId must be grater of equal to zero");
        }

        return ColumnExpression.Builder.of(alias, ResolvedType.of(Type.Any))
                .withLambdaId(lambdaId)
                .build();
    }

    /** Create an {@link ColumnExpression} with lambda id and type */
    protected ColumnExpression lce(String alias, int lambdaId, ResolvedType type)
    {
        if (lambdaId < 0)
        {
            throw new IllegalArgumentException("lambdaId must be grater of equal to zero");
        }

        return ColumnExpression.Builder.of(alias, type)
                .withLambdaId(lambdaId)
                .build();
    }

    /** Create an aggregate wrapper expression */
    protected IAggregateExpression agg(IExpression expression, boolean singleValue)
    {
        return new AggregateWrapperExpression(expression, singleValue, false);
    }

    /** Create an {@link ColumnExpression} with no ordinal and outer reference */
    protected ColumnExpression ocre(String column, TableSourceReference tr)
    {
        return ocre(column, tr, ResolvedType.of(Type.Any), CoreColumn.Type.REGULAR);
    }

    /** Create an {@link ColumnExpression} with type, no ordinal and outer reference */
    protected ColumnExpression ocre(String column, TableSourceReference tr, ResolvedType type, CoreColumn.Type columnType)
    {
        return ColumnExpression.Builder.of(column, type)
                .withColumnReference(new ColumnReference(tr, columnType))
                .withColumn(column)
                .withOuterReference(true)
                .build();
    }

    /** Create an {@link ColumnExpression} with ordinal and outer reference */
    protected ColumnExpression ocre(String alias, TableSourceReference tr, int ordinal, ResolvedType type)
    {
        return ocre(alias, tr, ordinal, type, CoreColumn.Type.REGULAR);
    }

    /** Create an {@link ColumnExpression} with ordinal and outer reference */
    protected ColumnExpression ocre(String alias, TableSourceReference tr, int ordinal, ResolvedType type, CoreColumn.Type columnType)
    {
        if (ordinal < 0)
        {
            throw new IllegalArgumentException("Ordinal must be grater of equal to zero");
        }
        return ColumnExpression.Builder.of(alias, type)
                .withOuterReference(true)
                .withColumnReference(new ColumnReference(tr, columnType))
                .withOrdinal(ordinal)
                .build();
    }

    /** Create an {@link ColumnExpression} with column and outer reference and alias */
    protected ColumnExpression ocre(String alias, TableSourceReference tr, String column, ResolvedType type, CoreColumn.Type columnType)
    {
        return ColumnExpression.Builder.of(alias, type)
                .withOuterReference(true)
                .withColumnReference(new ColumnReference(tr, columnType))
                .withColumn(column)
                .build();
    }

    protected IExpression and(IExpression left, IExpression right)
    {
        return new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, left, right);
    }

    protected IExpression or(IExpression left, IExpression right)
    {
        return new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, left, right);
    }

    protected IExpression gt(IExpression left, IExpression right)
    {
        return new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN, left, right);
    }

    protected IExpression lt(IExpression left, IExpression right)
    {
        return new ComparisonExpression(IComparisonExpression.Type.LESS_THAN, left, right);
    }

    protected IExpression eq(IExpression left, IExpression right)
    {
        return new ComparisonExpression(IComparisonExpression.Type.EQUAL, left, right);
    }

    protected IExpression neq(IExpression left, IExpression right)
    {
        return new ComparisonExpression(IComparisonExpression.Type.NOT_EQUAL, left, right);
    }

    protected IExpression add(IExpression left, IExpression right)
    {
        return new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, left, right);
    }

    protected IExpression intLit(int value)
    {
        return new LiteralIntegerExpression(value);
    }

    protected IExpression stringLit(String value)
    {
        return new LiteralStringExpression(value);
    }

    protected IExpression not(IExpression expression)
    {
        return new LogicalNotExpression(expression);
    }

    protected IExpression nullP(IExpression expression, boolean not)
    {
        return new NullPredicateExpression(expression, not);
    }

    protected IExpression in(IExpression expression, List<IExpression> arguments, boolean not)
    {
        return new InExpression(expression, arguments, not);
    }

    protected IExpression like(IExpression expression, IExpression patternExpression, boolean not)
    {
        return new LikeExpression(expression, patternExpression, not, null);
    }

    protected Schema schema(String... cols)
    {
        Type[] types = new Type[cols.length];
        for (int i = 0; i < cols.length; i++)
        {
            types[i] = Type.Any;
        }
        return schema(types, cols);
    }

    /** Create a schema */
    protected Schema schema(Column.Type[] types, String... cols)
    {
        if (types.length != cols.length)
        {
            throw new IllegalArgumentException("Types and columns must equal in size");
        }

        List<Column> columns = new ArrayList<>(cols.length);
        for (int i = 0; i < cols.length; i++)
        {
            columns.add(new Column(cols[i], ResolvedType.of(types[i])));
        }
        return new Schema(columns);
    }

    /** Construct a column */
    protected CoreColumn col(String name, Type type, TableSourceReference tableSourceReference)
    {
        return CoreColumn.of(name, ResolvedType.of(type), tableSourceReference);
    }

    protected CoreColumn col(String name, ResolvedType type, TableSourceReference tableSourceReference)
    {
        return CoreColumn.of(name, type, tableSourceReference);
    }

    protected CoreColumn col(String name, ResolvedType type, TableSourceReference tableSourceReference, boolean internal)
    {
        return new CoreColumn(name, type, "", internal, tableSourceReference, CoreColumn.Type.REGULAR);
    }

    /** Construct a column */
    protected CoreColumn col(String name, Type type)
    {
        return CoreColumn.of(name, ResolvedType.of(type), null);
    }

    protected CoreColumn pop(String name, ResolvedType type, TableSourceReference tableSourceReference)
    {
        return new CoreColumn(name, type, "", false, tableSourceReference, CoreColumn.Type.POPULATED);
    }

    // CSOFF
    /** Construct a column */
    protected CoreColumn ast(String name, Type type, TableSourceReference tableSourceReference)
    {
        return new CoreColumn(name, ResolvedType.of(type), "", false, tableSourceReference, CoreColumn.Type.ASTERISK);
    }

    protected CoreColumn ast(String name, String outputName, Type type, TableSourceReference tableSourceReference)
    {
        return new CoreColumn(name, ResolvedType.of(type), outputName, false, tableSourceReference, CoreColumn.Type.ASTERISK);
    }

    /** Construct a column */
    protected CoreColumn ast(String name, TableSourceReference tableSourceReference)
    {
        return new CoreColumn(name, ResolvedType.of(Type.Any), "", false, tableSourceReference, CoreColumn.Type.ASTERISK);
    }

    protected CoreColumn ast(String name, ResolvedType type, TableSourceReference tableSourceReference)
    {
        return new CoreColumn(name, type, "", false, tableSourceReference, CoreColumn.Type.ASTERISK);
    }

    // CSOFF

    protected void assertExpression(Object expected, Map<String, Object> values, String expression) throws Exception
    {
        ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));
        assertExpression(context, expected, values, expression, false);
    }

    protected void assertExpression(Object expected, Map<String, Object> values, String expression, boolean rethrow) throws Exception
    {
        ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));
        assertExpression(context, expected, values, expression, rethrow);
    }

    protected void assertExpression(ExecutionContext context, Object expected, java.util.Map<String, Object> variables, String expression) throws Exception
    {
        assertExpression(context, expected, variables, expression, false);
    }

    protected void assertExpression(ExecutionContext context, Object expected, java.util.Map<String, Object> variables, String expression, boolean rethrow) throws Exception
    {
        try
        {
            IExpression expr = e(expression);
            expr = LogicalPlanOptimizer.resolveExpression(context, expr);

            TupleVector input = TupleVector.CONSTANT;

            if (variables != null)
            {
                for (Entry<String, Object> e : variables.entrySet())
                {
                    context.setVariable(e.getKey()
                            .toLowerCase(), ValueVector.literalAny(e.getValue()));
                }
            }

            ValueVector vv = expr.eval(input, context);
            Object actual = vv.valueAsObject(0);
            if (vv.type()
                    .getType() == Type.DateTime)
            {
                actual = vv.getDateTime(0)
                        .getLocalDateTime();
            }
            else if (expected instanceof String)
            {
                actual = String.valueOf(actual);
            }
            else if (actual instanceof ValueVector
                    && !(actual instanceof UTF8String
                            || actual instanceof Decimal
                            || actual instanceof EpochDateTime
                            || actual instanceof EpochDateTimeOffset))
            {
                ValueVector v = (ValueVector) actual;
                List<Object> list = new ArrayList<>(v.size());
                for (int i = 0; i < v.size(); i++)
                {
                    list.add(v.valueAsObject(i));
                }
                actual = list;
            }

            if (actual instanceof byte[] bytes)
            {
                assertArrayEquals("Eval: " + expression, (byte[]) expected, bytes);
            }
            else
            {
                assertEquals("Eval: " + expression, expected, actual);
            }
        }
        catch (Exception e)
        {
            if (rethrow)
            {
                throw e;
            }
            e.printStackTrace();
            fail(expression + " " + e.getMessage());
        }
    }
}
