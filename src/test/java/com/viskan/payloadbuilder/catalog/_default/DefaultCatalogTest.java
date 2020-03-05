package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.codegen.CodeGenerator;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Arrays.asList;

import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Assert;
import org.junit.Test;

/** Tests functions etc. in default catalog */
public class DefaultCatalogTest extends Assert
{
    private final CodeGenerator codeGenerator = new CodeGenerator();
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();

    @Test
    public void test_function_hash()
    {
        assertFunction(null, null, "hash(null)");
        assertFunction(null, null, "hash(null,true)");
        assertFunction(1262, null, "hash(true)");
        assertFunction(1115, null, "hash(1,123)");
    }
    
    @Test
    public void test_function_now()
    {
        assertFunction(true, null, "now() > 0");
    }

    @Test
    public void test_function_filter()
    {
        TableAlias alias = TableAlias.of(null, "table", "t");
        alias.setColumns(new String[] {"a"});
        Row row = Row.of(alias, 0, new Object[] {asList(-1, -2, -3, 0, 1, 2, 3)});
        assertFunction(asList(1, 2, 3), row, "a.filter(a -> a > 0)");
        assertFunction(asList(2), row, "filter(a.filter(a -> a > 0), a -> a = 2)");
        assertFunction(asList("1s", "2s", "3s"), row, "map(a.filter(a -> a > 0), a -> a + 's')");
    }

    @Test
    public void test_function_map()
    {
        TableAlias alias = TableAlias.of(null, "table", "t");
        alias.setColumns(new String[] {"a"});
        Row row = Row.of(alias, 0, new Object[] {asList(-1, -2, -3, 0, 1, 2, 3)});
        assertFunction(asList(-2L, -4L, -6L, 0L, 2L, 4L, 6L), row, "a.map(a -> a * 2)");
        assertFunction(asList(-1L, -2L, -3L, 0L, 1L, 2L, 3L), row, "map(a.map(a -> a * 2), a -> a / 2)");
    }

    private void assertFunction(Object expected, Row row, String expression)
    {
        TableAlias alias = row == null ? TableAlias.of(null, "table", "t") : row.getTableAlias();
        Expression e = parser.parseExpression(catalogRegistry, expression);

        Object actual = codeGenerator.generateFunction(TableAlias.of(alias, "table", "t"), e).apply(row);
        if (actual instanceof Iterator)
        {
            actual = IteratorUtils.toList((Iterator) actual);
        }

        assertEquals("Code gen", expected, actual);
        
        actual = e.eval(new EvaluationContext(), row);
        if (actual instanceof Iterator)
        {
            actual = IteratorUtils.toList((Iterator) actual);
        }

        assertEquals("Eval", expected, actual);
    }
}
