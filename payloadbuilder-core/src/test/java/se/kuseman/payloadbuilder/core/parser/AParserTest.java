package se.kuseman.payloadbuilder.core.parser;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.operator.IIndexPredicate;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.session.IQuerySession;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.codegen.CodeGenerator;
import se.kuseman.payloadbuilder.core.operator.EvalUtils;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;
import se.kuseman.payloadbuilder.core.operator.OperatorBuilder;
import se.kuseman.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import se.kuseman.payloadbuilder.core.parser.rewrite.StatementResolver;

/** Base class for parser tests */
public abstract class AParserTest extends Assert
{
    protected static final CodeGenerator CODE_GENERATOR = new CodeGenerator();
    private final QueryParser parser = new QueryParser();
    protected final QuerySession session = new QuerySession(new CatalogRegistry());
    protected final ExecutionContext context = new ExecutionContext(session);

    /** Compile query with provided table creators */
    protected Operator operator(String query, Map<String, Function<TableAlias, Operator>> opByTable)
    {
        return operator(query, emptyMap(), emptyMap(), opByTable);
    }

    /** Compile query with provided table creators */
    protected Operator operator(String query, Map<String, Index> indexByTable, Map<String, Function<TableAlias, Operator>> indexOpByTable, Map<String, Function<TableAlias, Operator>> opByTable)
    {
        Select select = s(query);

        session.getCatalogRegistry()
                .registerCatalog("test", new Catalog("test")
                {
                    @Override
                    public List<Index> getIndices(IQuerySession session, String catalogAlias, QualifiedName table)
                    {
                        Index index = indexByTable.get(table.toDotDelimited());
                        return index != null ? singletonList(index)
                                : emptyList();
                    }

                    @Override
                    public Operator getIndexOperator(OperatorData data, IIndexPredicate index)
                    {
                        String table = data.getTableAlias()
                                .getTable()
                                .toDotDelimited();
                        return Optional.ofNullable(indexOpByTable.get(table))
                                .map(t -> t.apply(data.getTableAlias()))
                                .orElseThrow(() -> new RuntimeException("No index operator found for " + table + ", check test data."));
                    }

                    @Override
                    public Operator getScanOperator(OperatorData data)
                    {
                        String table = data.getTableAlias()
                                .getTable()
                                .toDotDelimited();
                        return Optional.ofNullable(opByTable.get(table))
                                .map(t -> t.apply(data.getTableAlias()))
                                .orElseThrow(() -> new RuntimeException("No scan operator found for " + table + ", check test data."));
                    }
                });

        session.setDefaultCatalogAlias("test");

        return OperatorBuilder.create(session, select)
                .getOperator();
    }

    // CSOFF
    protected QueryStatement q(String query)
    // CSON
    {
        QueryStatement statement = parser.parseQuery(query);
        return StatementResolver.resolve(statement, session);
    }

    // CSOFF
    protected Select s(String query)
    // CSON
    {
        Select selecct = parser.parseSelect(query);
        return StatementResolver.resolve(selecct, session);
    }

    /** Parse expression with mocked valued from provided tuple */
    // CSOFF
    protected Expression e(String expression, final Tuple tuple, final Map<String, Pair<Object, DataType>> types)
    // CSON
    {
        AtomicInteger colCount = new AtomicInteger();
        return StatementResolver.resolve(parser.parseExpression(expression), session, e ->
        {
            int col = colCount.get();
            String colName = e.getQname()
                    .toDotDelimited();
            Pair<Object, DataType> pair = types.get(colName);

            when(tuple.getColumnOrdinal(colName)).thenReturn(col);

            boolean isNull = pair.getKey() == null;
            when(tuple.isNull(col)).thenReturn(isNull);
            if (!isNull
                    && pair.getValue() == DataType.INT)
            {
                when(tuple.getInt(col)).thenReturn((Integer) pair.getKey());
            }
            else if (!isNull
                    && pair.getValue() == DataType.LONG)
            {
                when(tuple.getLong(col)).thenReturn((Long) pair.getKey());
            }
            else if (!isNull
                    && pair.getValue() == DataType.FLOAT)
            {
                when(tuple.getFloat(col)).thenReturn((Float) pair.getKey());
            }
            else if (!isNull
                    && pair.getValue() == DataType.DOUBLE)
            {
                when(tuple.getDouble(col)).thenReturn((Double) pair.getKey());
            }
            else if (!isNull
                    && pair.getValue() == DataType.BOOLEAN)
            {
                when(tuple.getBool(col)).thenReturn((Boolean) pair.getKey());
            }
            when(tuple.getValue(col)).thenReturn(pair.getKey());
            colCount.incrementAndGet();
            ResolvePath[] resolvePaths = new ResolvePath[] { new ResolvePath(-1, -1, asList(colName), -1, pair.getValue()) };
            return new QualifiedReferenceExpression(e.getQname(), e.getLambdaId(), resolvePaths, e.getToken());
        });
    }

    // CSOFF
    protected Expression e(String expression)
    // CSON
    {
        Expression e = parser.parseExpression(expression);
        return StatementResolver.resolve(e, session);
    }

    // CSOFF
    protected Expression e(String expression, TableAlias alias)
    // CSON
    {
        Expression e = parser.parseExpression(expression);
        return StatementResolver.resolve(e, alias);
    }

    /** Parse expression but do no resolving */
    protected Expression en(String expression)
    {
        return parser.parseExpression(expression);
    }

    protected void assertQueryFail(Class<? extends Exception> expected, String messageContains, String query)
    {
        try
        {
            q(query);
            fail("Query should fail with " + expected + " containing message: " + messageContains);
        }
        catch (Exception e)
        {
            if (!expected.isAssignableFrom(e.getClass()))
            {
                throw e;
            }
            assertTrue(e.getMessage(), e.getMessage()
                    .contains(messageContains));
        }
    }

    protected void assertExpressionFail(Class<? extends Exception> e, String messageContains, Map<String, Object> mapValues, String expression)
    {
        try
        {
            Expression expr = parser.parseExpression(expression);
            expr = StatementResolver.resolve(expr, session);
            ExecutionContext context = new ExecutionContext(session);
            if (mapValues != null)
            {
                String[] columns = new String[mapValues.size()];
                Object[] values = new Object[mapValues.size()];

                MutableInt index = new MutableInt();
                mapValues.entrySet()
                        .stream()
                        .forEach(en ->
                        {
                            columns[index.intValue()] = lowerCase(en.getKey());
                            values[index.getAndIncrement()] = en.getValue();
                        });

                context.getStatementContext()
                        .setTuple(new Tuple()
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
            if (!ee.getClass()
                    .isAssignableFrom(e))
            {
                throw ee;
            }

            assertTrue("Expected exception message to contain " + messageContains + " but was: " + ee.getMessage(), ee.getMessage()
                    .contains(messageContains));
        }
    }

    /** Tests both eval and code gen for a function expression */
    protected void assertFunction(Object expected, Map<String, Object> values, String expression) throws Exception
    {
        assertExpression(expected, values, expression, false, true);
    }

    /** Tests both eval and code gen for a predicate expression */
    protected void assertPredicate(Boolean expected, Map<String, Object> values, String expression) throws Exception
    {
        assertExpression(expected, values, expression, true, false);
    }

    protected void assertExpression(Object expected, Map<String, Object> values, String expression) throws Exception
    {
        assertExpression(expected, values, expression, false, false);
    }

    protected void assertExpression(Object expected, Map<String, Object> mapValues, String expression, boolean predicate, boolean function) throws Exception
    {
        try
        {
            Expression expr = parser.parseExpression(expression);
            expr = StatementResolver.resolve(expr, session);
            ExecutionContext context = new ExecutionContext(session);
            if (mapValues != null)
            {
                String[] columns = new String[mapValues.size()];
                Object[] values = new Object[mapValues.size()];

                MutableInt index = new MutableInt();
                mapValues.entrySet()
                        .stream()
                        .forEach(en ->
                        {
                            columns[index.intValue()] = lowerCase(en.getKey());
                            values[index.getAndIncrement()] = en.getValue();
                        });

                context.getStatementContext()
                        .setTuple(new Tuple()
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
                                return ArrayUtils.indexOf(columns, lowerCase(column));
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
                assertEquals("Eval: " + expression, expected, actual == null ? false
                        : actual);
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
}
