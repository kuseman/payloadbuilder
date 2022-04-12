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

/** Unit test of {@link ComparisonExpression} */
public class ComparisonExpressionTest extends AParserTest
{
    @Test
    public void test_codeGen_eq()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext()
                .setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int = null
        e = e("1 = a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int = int
        e = e("10 = a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int = long
        e = e("10 = a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int = float
        e = e("10 = a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int = double
        e = e("10 = a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int = Object (int)
        e = e("10 = a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean = boolean
        e = e("true = a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(true, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean = null
        e = e("true = a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));
    }

    @Test
    public void test_codeGen_neq()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext()
                .setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int != null
        e = e("9 != a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int != int
        e = e("9 != a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int != long
        e = e("9 != a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int != float
        e = e("9 != a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int != double
        e = e("9 != a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int != Object (int)
        e = e("9 != a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean != boolean
        e = e("false != a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(true, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean != null
        e = e("true != a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));
    }

    @Test
    public void test_codeGen_gt()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext()
                .setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int > null
        e = e("11 > a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int > int
        e = e("11 > a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int > long
        e = e("11 > a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int > float
        e = e("11 > a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int > double
        e = e("11 > a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int > Object (int)
        e = e("11 > a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // double > int
        e = e("11d > a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean > boolean
        e = e("false > a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(true, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // boolean > null
        e = e("true > a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));
    }

    @Test
    public void test_codeGen_gte()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext()
                .setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int >= null
        e = e("10 >= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int >= int
        e = e("10 >= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int >= long
        e = e("10 >= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int >= float
        e = e("10 >= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int >= double
        e = e("10 >= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int >= Object (int)
        e = e("10 >= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // double >= int
        e = e("10d >= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean >= boolean
        e = e("false >= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(true, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // boolean >= null
        e = e("true >= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));
    }

    @Test
    public void test_codeGen_lt()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext()
                .setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int < null
        e = e("9 < a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int < int
        e = e("9 < a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int < long
        e = e("9 < a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int < float
        e = e("9 < a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int < double
        e = e("9 < a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int < Object (int)
        e = e("9 < a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // double < int
        e = e("9d < a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean < boolean
        e = e("false < a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(true, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean < null
        e = e("true < a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));
    }

    @Test
    public void test_codeGen_lte()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext()
                .setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int <= null
        e = e("9 <= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int <= int
        e = e("9 <= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int <= long
        e = e("9 <= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int <= float
        e = e("9 <= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int <= double
        e = e("9 <= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int <= Object (int)
        e = e("9 <= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // double <= int
        e = e("9d <= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean <= boolean
        e = e("false <= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(true, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // boolean <= null
        e = e("true <= a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.BOOLEAN))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));
    }

    @Test
    public void test_fold()
    {
        Expression e;

        e = e("1=1");
        assertTrue(e.isConstant());
        assertEquals(TRUE_LITERAL, e);
        e = e("1 > 1");
        assertEquals(FALSE_LITERAL, e);
        e = e("1=a");
        assertFalse(e.isConstant());
        assertEquals(e("1=a"), e);
        e = e("a=1");
        assertEquals(e("a=1"), e);
        assertFalse(e.isConstant());

        e = e("null=1");
        assertEquals(NULL_LITERAL, e);
        e = e("1=null");
        assertEquals(NULL_LITERAL, e);
        e = e("(1+2) > 10");
        assertEquals(FALSE_LITERAL, e);
        e = e("10 > (1+2)");
        assertEquals(TRUE_LITERAL, e);
        e = e("(1+2+a) > 10");
        assertEquals(e("(3+a) > 10"), e);
    }
}
