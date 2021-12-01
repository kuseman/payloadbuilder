package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Unit test of {@link ArithmeticUnaryExpression} */
public class ArithmeticUnaryExpressionTest extends AParserTest
{
    @Test
    public void test_codeGen()
    {
        Tuple tuple = mock(Tuple.class);
        Map<String, Pair<Object, DataType>> types = MapUtils.ofEntries(
                MapUtils.entry("a", Pair.of(1, DataType.INT)),
                MapUtils.entry("b", Pair.of(1L, DataType.LONG)),
                MapUtils.entry("c", Pair.of(1f, DataType.FLOAT)),
                MapUtils.entry("d", Pair.of(1d, DataType.DOUBLE)),
                MapUtils.entry("e", Pair.of(new Integer(1), DataType.ANY)),
                MapUtils.entry("f", Pair.of(new Long(1), DataType.ANY)),
                MapUtils.entry("g", Pair.of(new Float(1), DataType.ANY)),
                MapUtils.entry("h", Pair.of(new Double(1), DataType.ANY)));
        List<Expression> es = asList(
                e("-a < 0"),
                e("-b < 0"),
                e("-c < 0"),
                e("-d < 0"),
                e("-e < 0"),
                e("-f < 0"),
                e("-g < 0"),
                e("-h < 0"));

        context.getStatementContext().setTuple(tuple);
        for (Expression e : es)
        {
            setup(tuple, e, types);
            Predicate<ExecutionContext> predicate = CODE_GENERATOR.generatePredicate(e);
            assertTrue(e.toString(), predicate.test(context));
        }
    }

    @Test
    public void test_fold()
    {
        Expression e;

        e = e("-(1+2)");
        assertTrue(e.isConstant());
        assertEquals(e("-3"), e);

        e = e("-null");
        assertEquals(NULL_LITERAL, e);

        e = e("-a");
        assertFalse(e.isConstant());
        assertEquals(e("-a"), e);

        e = e("-(1+2+a)");
        assertEquals(e("-(3+a)"), e);
    }
}
