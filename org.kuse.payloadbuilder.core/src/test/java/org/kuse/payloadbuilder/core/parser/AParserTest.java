package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.codegen.CodeGenerator;
import org.kuse.payloadbuilder.core.operator.EvalUtils;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.OperatorBuilder;
import org.kuse.payloadbuilder.core.operator.SelectResolver;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;

/** Base class for parser tests */
public abstract class AParserTest extends Assert
{
    protected static final CodeGenerator CODE_GENERATOR = new CodeGenerator();
    private final QueryParser p = new QueryParser();
    protected final QuerySession session = new QuerySession(new CatalogRegistry());
    protected final ExecutionContext context = new ExecutionContext(session);

    protected QueryStatement q(String query)
    {
        return p.parseQuery(session.getCatalogRegistry(), query);
    }

    /** Compile query with provided table creators */
    protected Operator operator(String query, Map<String, Function<TableAlias, Operator>> opByTable)
    {
        return operator(query, emptyMap(), emptyMap(), opByTable);
    }

    /** Compile query with provided table creators */
    protected Operator operator(
            String query,
            Map<String, Index> indexByTable,
            Map<String, Function<TableAlias, Operator>> indexOpByTable,
            Map<String, Function<TableAlias, Operator>> opByTable)
    {
        Select select = p.parseSelect(session.getCatalogRegistry(), query);

        session.getCatalogRegistry().registerCatalog("test", new Catalog("test")
        {
            @Override
            public List<Index> getIndices(QuerySession session, String catalogAlias, QualifiedName table)
            {
                Index index = indexByTable.get(table.toString());
                return index != null ? singletonList(index) : emptyList();
            }

            @Override
            public Operator getIndexOperator(OperatorData data, Index index)
            {
                String table = data.getTableAlias().getTable().toString();
                return Optional.ofNullable(indexOpByTable.get(table))
                        .map(t -> t.apply(data.getTableAlias()))
                        .orElseThrow(() -> new RuntimeException("No index operator found for " + table + ", check test data."));
            }

            @Override
            public Operator getScanOperator(OperatorData data)
            {
                String table = data.getTableAlias().getTable().toString();
                return Optional.ofNullable(opByTable.get(table))
                        .map(t -> t.apply(data.getTableAlias()))
                        .orElseThrow(() -> new RuntimeException("No scan operator found for " + table + ", check test data."));
            }
        });

        session.getCatalogRegistry().setDefaultCatalog("test");

        return OperatorBuilder.create(session, select).getOperator();
    }

    protected Select s(String query)
    {
        return p.parseSelect(session.getCatalogRegistry(), query);
    }

    protected Expression e(String expression)
    {
        return p.parseExpression(session.getCatalogRegistry(), expression);
    }

    protected Expression e(String expression, TableAlias alias)
    {
        Expression e = e(expression);
        // Resolve expression with provided alias
        SelectResolver.resolveTorTest(e, alias);
        return e;
    }

    protected void assertExpressionFail(Class<? extends Exception> e, String messageContains, Map<String, Object> mapValues, String expression)
    {
        try
        {
            Expression expr = p.parseExpression(session.getCatalogRegistry(), expression);
            ExecutionContext context = new ExecutionContext(session);
            if (mapValues != null)
            {
                String[] columns = new String[mapValues.size()];
                Object[] values = new Object[mapValues.size()];

                MutableInt index = new MutableInt();
                mapValues.entrySet().stream().forEach(en ->
                {
                    columns[index.intValue()] = lowerCase(en.getKey());
                    values[index.getAndIncrement()] = en.getValue();
                });

                context.getStatementContext().setTuple(new Tuple()
                {
                    @Override
                    public int getTupleOrdinal()
                    {
                        return 0;
                    }

                    @Override
                    public int getColumnCount()
                    {
                        return columns.length;
                    }

                    @Override
                    public String getColumn(int columnOrdinal)
                    {
                        return columns[columnOrdinal];
                    }

                    @Override
                    public int getColumnOrdinal(String column)
                    {
                        return ArrayUtils.indexOf(columns, column);
                    }

                    @Override
                    public Object getValue(int columnOrdinal)
                    {
                        return values[columnOrdinal];
                    }

                    @Override
                    public Tuple getTuple(int tupleOrdinal)
                    {
                        return null;
                    }
                });
            }
            expr.eval(context);
            fail(expression + " should fail.");
        }
        catch (Exception ee)
        {
            if (!ee.getClass().isAssignableFrom(e))
            {
                throw ee;
            }

            assertTrue("Expected exception message to contain " + messageContains + " but was: " + ee.getMessage(), ee.getMessage().contains(messageContains));
        }
    }

    /** Tests both eval and code gen for a function expression */
    protected void assertFunction(Object expected, Map<String, Object> values, String expression) throws Exception
    {
        assertExpression(expected, values, expression, false, true);
    }

    protected void assertExpression(Object expected, Map<String, Object> values, String expression) throws Exception
    {
        assertExpression(expected, values, expression, false, false);
    }

    /** Tests both eval and code gen for a predicate expression */
    protected void assertPredicate(Boolean expected, Map<String, Object> values, String expression) throws Exception
    {
        assertExpression(expected, values, expression, true, false);
    }

    protected void assertExpression(Object expected, Map<String, Object> mapValues, String expression, boolean predicate, boolean function) throws Exception
    {
        try
        {
            Expression expr = p.parseExpression(session.getCatalogRegistry(), expression);
            ExecutionContext context = new ExecutionContext(session);
            if (mapValues != null)
            {
                String[] columns = new String[mapValues.size()];
                Object[] values = new Object[mapValues.size()];

                MutableInt index = new MutableInt();
                mapValues.entrySet().stream().forEach(en ->
                {
                    columns[index.intValue()] = lowerCase(en.getKey());
                    values[index.getAndIncrement()] = en.getValue();
                });

                context.getStatementContext().setTuple(new Tuple()
                {
                    Map<Integer, Object> iteratorCache = new HashMap<>();

                    @Override
                    public int getTupleOrdinal()
                    {
                        return 0;
                    }

                    @Override
                    public int getColumnCount()
                    {
                        return columns.length;
                    }

                    @Override
                    public String getColumn(int columnOrdinal)
                    {
                        return columns[columnOrdinal];
                    }

                    @Override
                    public int getColumnOrdinal(String column)
                    {
                        return ArrayUtils.indexOf(columns, column);
                    }

                    @Override
                    public Object getValue(int columnOrdinal)
                    {
                        Object v = iteratorCache.get(columnOrdinal);
                        if (v == null)
                        {
                            v = values[columnOrdinal];
                        }

                        // To be able to test an iterator multiple times, store the
                        // iterator and rewind
                        if (v instanceof Iterator)
                        {
                            @SuppressWarnings("unchecked")
                            List<Object> l = IteratorUtils.toList((Iterator<Object>) v);
                            iteratorCache.put(columnOrdinal, l.iterator());
                            v = l.iterator();
                        }

                        return v;
                    }

                    @Override
                    public Tuple getTuple(int ordinal)
                    {
                        return null;
                    }
                });
            }

            Object actual = EvalUtils.unwrap(context, expr.eval(context));
            if (predicate)
            {
                assertEquals("Eval: " + expression, expected, actual == null ? false : actual);
            }
            else
            {
                assertEquals("Eval: " + expression, expected, actual);
            }

            if (predicate)
            {
                Predicate<ExecutionContext> p = CODE_GENERATOR.generatePredicate(expr);
                assertEquals("Code gen: " + expression, expected, p.test(context));
            }
            else if (function)
            {
                Function<ExecutionContext, Object> f = CODE_GENERATOR.generateFunction(expr);
                assertEquals("Code gen: " + expression, expected, EvalUtils.unwrap(context, f.apply(context)));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(expression + " " + e.getMessage());
        }
    }

    /** Set up qualified expressions and tuple with values and types */
    protected void setup(Tuple tuple, Expression e, Map<String, Pair<Object, DataType>> types)
    {
        e.accept(new AExpressionVisitor<Void, Void>()
        {
            int col;

            @SuppressWarnings("deprecation")
            @Override
            public Void visit(QualifiedReferenceExpression e, Void context)
            {
                String colName = e.getQname().toString();
                Pair<Object, DataType> pair = types.get(colName);
                e.setResolvePaths(asList(new ResolvePath(-1, -1, asList(colName), -1, new int[0], pair.getValue())));
                when(tuple.getColumnOrdinal(colName)).thenReturn(col);

                boolean isNull = pair.getKey() == null;
                when(tuple.isNull(col)).thenReturn(isNull);
                if (!isNull && pair.getValue() == DataType.INT)
                {
                    when(tuple.getInt(col)).thenReturn((Integer) pair.getKey());
                }
                else if (!isNull && pair.getValue() == DataType.LONG)
                {
                    when(tuple.getLong(col)).thenReturn((Long) pair.getKey());
                }
                else if (!isNull && pair.getValue() == DataType.FLOAT)
                {
                    when(tuple.getFloat(col)).thenReturn((Float) pair.getKey());
                }
                else if (!isNull && pair.getValue() == DataType.DOUBLE)
                {
                    when(tuple.getDouble(col)).thenReturn((Double) pair.getKey());
                }
                else if (!isNull && pair.getValue() == DataType.BOOLEAN)
                {
                    when(tuple.getBool(col)).thenReturn((Boolean) pair.getKey());
                }
                when(tuple.getValue(col)).thenReturn(pair.getKey());
                col++;
                return null;
            }
        }, null);
    }
}
