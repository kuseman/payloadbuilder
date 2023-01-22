package se.kuseman.payloadbuilder.core.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
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
        if (ordinal < 0)
        {
            throw new IllegalArgumentException("Ordinal must be grater of equal to zero");
        }
        return ColumnExpression.Builder.of(alias, ResolvedType.of(Type.Any))
                .withOrdinal(ordinal)
                .build();
    }

    /** Return a simple column expression that resolves values by ordinal with type */
    protected ColumnExpression ce(String alias, ResolvedType type, int ordinal)
    {
        if (ordinal < 0)
        {
            throw new IllegalArgumentException("Ordinal must be grater of equal to zero");
        }
        return ColumnExpression.Builder.of(alias, type)
                .withOrdinal(ordinal)
                .build();
    }

    /** Return a simple column expression that resolves values by ordinal in outer tuple */
    protected ColumnExpression oce(String alias, int ordinal)
    {
        if (ordinal < 0)
        {
            throw new IllegalArgumentException("Ordinal must be grater of equal to zero");
        }
        return ColumnExpression.Builder.of(alias, ResolvedType.of(Type.Any))
                .withOrdinal(ordinal)
                .withOuterReference(true)
                .build();
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(ColumnReference cr, int ordinal)
    {
        if (ordinal < 0)
        {
            throw new IllegalArgumentException("Ordinal must be grater of equal to zero");
        }
        return ColumnExpression.Builder.of(cr.getName(), ResolvedType.of(Type.Any))
                .withOrdinal(ordinal)
                .withColumnReference(cr)
                .build();
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(ColumnReference cr, int ordinal, ResolvedType type)
    {
        if (ordinal < 0)
        {
            throw new IllegalArgumentException("Ordinal must be grater of equal to zero");
        }
        return ColumnExpression.Builder.of(cr.getName(), type)
                .withOrdinal(ordinal)
                .withColumnReference(cr)
                .build();
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String alias, ColumnReference cr, int ordinal, ResolvedType type)
    {
        if (ordinal < 0)
        {
            throw new IllegalArgumentException("Ordinal must be grater of equal to zero");
        }
        return ColumnExpression.Builder.of(alias, type)
                .withOrdinal(ordinal)
                .withColumnReference(cr)
                .build();
    }

    /** Create an {@link ColumnExpression} */
    protected ColumnExpression cre(String alias, ColumnReference cr, String column, ResolvedType type)
    {
        return ColumnExpression.Builder.of(alias, type)
                .withColumn(column)
                .withColumnReference(cr)
                .build();
    }

    /** Create an {@link ColumnExpression} with no ordinal */
    protected ColumnExpression cre(ColumnReference cr)
    {
        if (cr.isAsterisk())
        {
            return ColumnExpression.Builder.of(cr.getName(), ResolvedType.of(Type.Any))
                    .withColumnReference(cr)
                    .build();
        }
        return ColumnExpression.Builder.of(cr.getName(), ResolvedType.of(Type.Any))
                .withColumnReference(cr)
                .withColumn(cr.getName())
                .build();
    }

    /** Create an {@link ColumnExpression} with no ordinal */
    protected ColumnExpression cre(String alias, ColumnReference cr, String column)
    {
        return ColumnExpression.Builder.of(alias, ResolvedType.of(Type.Any))
                .withColumnReference(cr)
                .withColumn(column)
                .build();
    }

    /** Create an {@link ColumnExpression} with no ordinal with type */
    protected ColumnExpression cre(ColumnReference cr, ResolvedType type)
    {
        return ColumnExpression.Builder.of(cr.getName(), type)
                .withColumnReference(cr)
                .withColumn(cr.getName())
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

    /** Create an {@link ColumnExpression} with lambda id and type and ordinal access */
    protected ColumnExpression lce(String alias, int lambdaId, ResolvedType type, int ordinal)
    {
        if (lambdaId < 0)
        {
            throw new IllegalArgumentException("lambdaId must be grater of equal to zero");
        }
        else if (ordinal < 0)
        {
            throw new IllegalArgumentException("ordinal must be grater of equal to zero");
        }

        return ColumnExpression.Builder.of(alias, type)
                .withLambdaId(lambdaId)
                .withOrdinal(ordinal)
                .build();
    }

    /** Create an {@link ColumnExpression} with lambda id and type and column access and column reference */
    protected ColumnExpression lce(String alias, int lambdaId, ResolvedType type, ColumnReference cr, String column)
    {
        if (lambdaId < 0)
        {
            throw new IllegalArgumentException("lambdaId must be grater of equal to zero");
        }

        return ColumnExpression.Builder.of(alias, type)
                .withLambdaId(lambdaId)
                .withColumn(column)
                .withColumnReference(cr)
                .build();
    }

    /** Create an aggregate wrapper expression */
    protected IAggregateExpression agg(IExpression expression, boolean singleValue)
    {
        return new AggregateWrapperExpression(expression, singleValue, false);
    }

    /** Create an {@link ColumnExpression} with no ordinal and outer reference */
    protected ColumnExpression ocre(ColumnReference cr)
    {
        return ColumnExpression.Builder.of(cr.getName(), ResolvedType.of(Type.Any))
                .withColumnReference(cr)
                .withColumn(cr.getName())
                .withOuterReference(true)
                .build();
    }

    /** Create an {@link ColumnExpression} with ordinal and outer reference */
    protected ColumnExpression ocre(ColumnReference cr, int ordinal)
    {
        return ColumnExpression.Builder.of(cr.getName(), ResolvedType.of(Type.Any))
                .withColumnReference(cr)
                .withOuterReference(true)
                .withOrdinal(ordinal)
                .build();
    }

    /** Create an {@link ColumnExpression} with ordinal and outer reference */
    protected ColumnExpression ocre(ColumnReference cr, int ordinal, ResolvedType type)
    {
        return ColumnExpression.Builder.of(cr.getName(), type)
                .withColumnReference(cr)
                .withOuterReference(true)
                .withOrdinal(ordinal)
                .build();
    }

    /** Create an {@link ColumnExpression} with ordinal and outer reference and alias */
    protected ColumnExpression ocre(String alias, ColumnReference cr, int ordinal, ResolvedType type)
    {
        return ColumnExpression.Builder.of(alias, type)
                .withColumnReference(cr)
                .withOuterReference(true)
                .withOrdinal(ordinal)
                .build();
    }

    /** Create an {@link ColumnExpression} with column and outer reference and alias */
    protected ColumnExpression ocre(String alias, ColumnReference cr, String column, ResolvedType type)
    {
        return ColumnExpression.Builder.of(alias, type)
                .withColumnReference(cr)
                .withOuterReference(true)
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

    protected IExpression add(IExpression left, IExpression right)
    {
        return new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, left, right);
    }

    protected IExpression intLit(int value)
    {
        return new LiteralIntegerExpression(value);
    }

    protected IExpression ast()
    {
        return new AsteriskExpression(null);
    }

    protected IExpression ast(String alias)
    {
        return new AsteriskExpression(QualifiedName.of(alias), null);
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
    protected Column col(String name, Type type, ColumnReference columnReference)
    {
        return new Column(name, ResolvedType.of(type), columnReference);
    }

    /** Construct a column */
    protected Column col(String name, Type type)
    {
        return new Column(name, ResolvedType.of(type), null, false);
    }

    protected Column col(ColumnReference reference, Type type)
    {
        return new Column(reference.getName(), ResolvedType.of(type), reference, false);
    }

    protected void assertExpression(Object expected, Map<String, Object> values, String expression) throws Exception
    {
        ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));
        assertExpression(context, expected, values, expression, false, false);
    }

    protected void assertExpression(ExecutionContext context, Object expected, java.util.Map<String, Object> variables, String expression, boolean predicate, boolean function) throws Exception
    {
        try
        {

            IExpression expr = e(expression);
            expr = LogicalPlanOptimizer.resolveExpression(context, expr);

            TupleVector input = null;

            if (variables != null)
            {
                for (Entry<String, Object> e : variables.entrySet())
                {
                    context.setVariable(e.getKey(), e.getValue());
                }
            }

            // if (mapValues != null)
            // {
            // String[] columns = new String[mapValues.size()];
            // Object[] values = new Object[mapValues.size()];
            //
            // MutableInt index = new MutableInt();
            // mapValues.entrySet()
            // .stream()
            // .forEach(en ->
            // {
            // columns[index.intValue()] = lowerCase(en.getKey());
            // values[index.getAndIncrement()] = en.getValue();
            // });
            //
            // final Schema schema = new Schema(Arrays.stream(columns)
            // .map(c -> Column.of(c, Type.Any))
            // .collect(toList()));
            //
            // input = new TupleVector()
            // {
            // @Override
            // public Schema getSchema()
            // {
            // return schema;
            // }
            //
            // @Override
            // public int getRowCount()
            // {
            // return mapValues.size();
            // }
            //
            // @Override
            // public ValueVector getColumn(int column)
            // {
            // return new ValueVector()
            // {
            //
            // @Override
            // public ResolvedType type()
            // {
            // return ResolvedType.of(Type.Any);
            // }
            //
            // @Override
            // public int size()
            // {
            // return mapValues.size();
            // }
            //
            // @Override
            // public boolean isNull(int row)
            // {
            // return values[row] == null;
            // }
            //
            // @Override
            // public Object getValue(int row)
            // {
            // return values[row];
            // }
            // };
            // }
            // };
            // }
            // else
            // {
            input = TupleVector.CONSTANT;
            // }

            ValueVector vv = expr.eval(input, context);
            if (predicate)
            {
                boolean actual = vv.getPredicateBoolean(0);
                assertEquals("Eval: " + expression, expected, actual);
            }
            else
            {
                Object actual = vv.valueAsObject(0);
                if (vv.type()
                        .getType() == Type.DateTime)
                {
                    actual = vv.getDateTime(0)
                            .toZonedDateTime();
                }
                else if (expected instanceof String)
                {
                    actual = String.valueOf(actual);
                }
                else if (actual instanceof ValueVector)
                {
                    ValueVector v = (ValueVector) actual;
                    List<Object> list = new ArrayList<>(v.size());
                    for (int i = 0; i < v.size(); i++)
                    {
                        list.add(v.valueAsObject(i));
                    }
                    actual = list;
                }

                assertEquals("Eval: " + expression, expected, actual);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(expression + " " + e.getMessage());
        }
    }
}
