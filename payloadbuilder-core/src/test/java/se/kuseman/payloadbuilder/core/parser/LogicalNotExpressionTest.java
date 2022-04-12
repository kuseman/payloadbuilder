package se.kuseman.payloadbuilder.core.parser;

import static org.mockito.Mockito.mock;
import static se.kuseman.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static se.kuseman.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static se.kuseman.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;

import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;

/** Unit test of {@link LogicalNotExpression} */
public class LogicalNotExpressionTest extends AParserTest
{
    @Test
    public void test_codeGen()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext()
                .setTuple(tuple);

        Expression e;
        Predicate<ExecutionContext> p;

        // not Object (boolean)
        e = e("not a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(false, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // not boolean
        e = e("not a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(false, DataType.ANY))));
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

        e = e("NOT a IN (1,2,3)");
        assertEquals(e("a NOT IN (1,2,3)"), e);

        e = e("NOT a NOT IN (1,2,3)");
        assertEquals(e("a IN (1,2,3)"), e);

        e = e("NOT a LIKE 'pat'");
        assertEquals(e("a NOT LIKE 'pat'"), e);

        e = e("NOT a NOT LIKE 'pat'");
        assertEquals(e("a LIKE 'pat'"), e);
    }
}
