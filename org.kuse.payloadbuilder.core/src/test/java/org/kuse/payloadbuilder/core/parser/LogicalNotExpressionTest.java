package org.kuse.payloadbuilder.core.parser;

import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;
import static org.mockito.Mockito.mock;

import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Unit test of {@link LogicalNotExpression} */
public class LogicalNotExpressionTest extends AParserTest
{
    @Test
    public void test_codeGen()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext().setTuple(tuple);

        Expression e;
        Predicate<ExecutionContext> p;

        // not Object (boolean)
        e = e("not a");
        setup(tuple, e, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(false, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // not boolean
        e = e("not a");
        setup(tuple, e, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(false, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));
    }

    @Test
    public void test_fold()
    {
        Expression e;

        e = e("not true");
        assertTrue(e.isConstant());
        assertEquals(FALSE_LITERAL, e);
        e = e("not false");
        assertEquals(TRUE_LITERAL, e);
        e = e("not a");
        assertFalse(e.isConstant());
        assertEquals(e("not a"), e);
        e = e("not null");
        assertEquals(NULL_LITERAL, e);

        e = e("not (true AND a)");
        assertFalse(e.isConstant());
        assertEquals(e("not a"), e);
    }
}
