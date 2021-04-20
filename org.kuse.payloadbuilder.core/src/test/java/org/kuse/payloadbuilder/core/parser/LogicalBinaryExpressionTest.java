package org.kuse.payloadbuilder.core.parser;

import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;
import static org.mockito.Mockito.mock;

import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Tuple;

/** Unit test of {@link LogicalBinaryExpression} */
public class LogicalBinaryExpressionTest extends AParserTest
{
    @Test
    public void test_cogeGen_and()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext().setTuple(tuple);

        Expression e;
        Predicate<ExecutionContext> p;

        // true and true
        e = e("a and b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(true, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // true and true (Object)
        e = e("a and b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(true, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // true and false
        e = e("a and b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(false, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // true and false (Object)
        e = e("a and b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(false, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // true and null
        e = e("a and b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // true and null (Object)
        e = e("a and b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(null, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // null and null
        e = e("a and b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(null, DataType.BOOLEAN)), entry("b", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // null and null (Object)
        e = e("a and b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(null, DataType.BOOLEAN)), entry("b", Pair.of(null, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));
    }

    @Test
    public void test_cogeGen_or()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext().setTuple(tuple);

        Expression e;
        Predicate<ExecutionContext> p;

        // true or true
        e = e("a or b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(true, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // true or true (Object)
        e = e("a or b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(true, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // true or false
        e = e("a or b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(false, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // true or false (Object)
        e = e("a or b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(false, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // true or null
        e = e("a or b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // true or null (Object)
        e = e("a or b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(true, DataType.BOOLEAN)), entry("b", Pair.of(null, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // null or null
        e = e("a or b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(null, DataType.BOOLEAN)), entry("b", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // null or null (Object)
        e = e("a or b");
        setup(tuple, e, ofEntries(entry("a", Pair.of(null, DataType.BOOLEAN)), entry("b", Pair.of(null, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));
    }

    @Test
    public void test_fold()
    {
        Expression e;

        e = e("true AND a");
        assertFalse(e.isConstant());
        assertEquals(e("a"), e);
        e = e("false AND a");
        assertEquals(FALSE_LITERAL, e);
        e = e("null AND a");
        assertEquals(e("null AND a"), e);
        e = e("null AND false");
        assertEquals(FALSE_LITERAL, e);

        e = e("true OR a");
        assertEquals(TRUE_LITERAL, e);
        e = e("false OR a");
        assertEquals(e("a"), e);
        e = e("null OR a");
        assertEquals(e("null oR a"), e);

        e = e("a AND true");
        assertFalse(e.isConstant());
        assertEquals(e("a"), e);
        e = e("a AND false");
        assertEquals(FALSE_LITERAL, e);
        e = e("a AND null");
        assertEquals(e("a AND null"), e);

        e = e("a OR true");
        assertEquals(TRUE_LITERAL, e);
        e = e("a OR false");
        assertEquals(e("a"), e);
        e = e("a OR null");
        assertEquals(e("a OR null"), e);

        e = e("a OR b");
        assertEquals(e("a OR b"), e);

        e = e("true OR false");
        assertTrue(e.isConstant());

        e = e("(a AND b AND c AND d) OR true");
        assertEquals(TRUE_LITERAL, e);

        e = e("(a AND b AND c AND d) OR false");
        assertEquals(e("(a AND b AND c AND d)"), e);

        e = e("(a AND true) OR (b and c)");
        assertFalse(e.isConstant());
        assertEquals(e("a OR (b and c)"), e);

        e = e("(pc.apply_to_articles = 1 or pc.includeSkuIds.contains(ap.sku_id)) and not pc.excludeSkuIds.contains(ap.sku_id)");
        assertFalse(e.isConstant());
        assertEquals(e("(pc.apply_to_articles = 1 or pc.includeSkuIds.contains(ap.sku_id)) and not pc.excludeSkuIds.contains(ap.sku_id)"), e);

        e = e("((a OR ((b))))");
        assertEquals(e("a OR b"), e);
    }
}
