package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;
import static org.mockito.Mockito.mock;

import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.ArithmeticBinaryExpression.Type;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Unit test of {@link ArithmeticBinaryExpression} */
public class ArithmeticBinaryExpressionTest extends AParserTest
{
    @Test
    public void test_dataType()
    {
        ArithmeticBinaryExpression e = new ArithmeticBinaryExpression(
                Type.ADD,
                LiteralExpression.create(null),
                LiteralExpression.create(10));

        assertEquals(DataType.ANY, e.getDataType());

        e = new ArithmeticBinaryExpression(
                Type.ADD,
                LiteralExpression.create(10),
                LiteralExpression.create(null));

        assertEquals(DataType.ANY, e.getDataType());

        e = new ArithmeticBinaryExpression(
                Type.ADD,
                LiteralExpression.create(10),
                LiteralExpression.create(10));

        assertEquals(DataType.INT, e.getDataType());

        e = new ArithmeticBinaryExpression(
                Type.ADD,
                LiteralExpression.create(10),
                LiteralExpression.create(10L));

        assertEquals(DataType.LONG, e.getDataType());

        e = new ArithmeticBinaryExpression(
                Type.ADD,
                LiteralExpression.create(10),
                LiteralExpression.create(10F));

        assertEquals(DataType.FLOAT, e.getDataType());

        e = new ArithmeticBinaryExpression(
                Type.ADD,
                LiteralExpression.create(10),
                LiteralExpression.create(10D));

        assertEquals(DataType.DOUBLE, e.getDataType());

        QualifiedReferenceExpression qre = new QualifiedReferenceExpression(
                QualifiedName.of("col"),
                -1,
                new ResolvePath[] {
                        new ResolvePath(-1, 0, asList("col"), -1, DataType.FLOAT)
                },
                null);
        e = new ArithmeticBinaryExpression(
                Type.ADD,
                qre,
                LiteralExpression.create(10));

        assertEquals(DataType.FLOAT, e.getDataType());
    }

    @Test
    public void test_codeGen_add()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext().setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int + null
        e = e("1 + a = 11", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int + int
        e = e("1 + a = 11", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int + long
        e = e("1 + a = 11l", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int + float
        e = e("1 + a = 11f", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int + double
        e = e("1 + a = 11d", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int + Object (int)
        e = e("1 + a = 11", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));
    }

    @Test
    public void test_codeGen_subtract()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext().setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int - null
        e = e("1 - a = -9", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int - int
        e = e("1 - a = -9", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int - long
        e = e("1 - a = -9l", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int - float
        e = e("1 - a = -9f", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int - double
        e = e("1 - a = -9d", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int - Object (int)
        e = e("1 - a = -9", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));
    }

    @Test
    public void test_codeGen_multiply()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext().setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int * null
        e = e("2 * a = 20", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int * int
        e = e("2 * a = 20", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int * long
        e = e("2 * a = 20l", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int * float
        e = e("2 * a = 20f", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int * double
        e = e("2 * a = 20d", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int * Object (int)
        e = e("2 * a = 20", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));
    }

    @Test
    public void test_codeGen_divide()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext().setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int / null
        e = e("20 / a = 2", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int / int
        e = e("20 / a = 2", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int / long
        e = e("20 / a = 2l", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int / float
        e = e("20 / a = 2f", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int / double
        e = e("20 / a = 2d", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int / Object (int)
        e = e("20 / a = 2", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));
    }

    @Test
    public void test_codeGen_modulo()
    {
        Tuple tuple = mock(Tuple.class);
        context.getStatementContext().setTuple(tuple);
        Expression e;
        Predicate<ExecutionContext> p;

        // int % null
        e = e("31 % a = 1", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(null, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertFalse(p.test(context));

        // int % int
        e = e("31 % a = 1", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.INT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int % long
        e = e("31 % a = 1l", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10L, DataType.LONG))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int % float
        e = e("31 % a = 1f", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10F, DataType.FLOAT))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int % double
        e = e("31 % a = 1d", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10D, DataType.DOUBLE))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));

        // int % Object (int)
        e = e("31 % a = 1", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(10, DataType.ANY))));
        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));
    }

    @Test
    public void test_fold()
    {
        Expression e;

        e = e("1+1");
        assertTrue(e.isConstant());
        assertEquals(e("2"), e);
        e = e("1+2+3+4+5+6");
        assertEquals(e("21"), e);
        e = e("1+2+3.1");
        assertEquals(e("6.1"), e);
        e = e("1+a");
        assertEquals(e("1+a"), e);
        assertFalse(e.isConstant());
        e = e("a+1");
        assertEquals(e("a+1"), e);
        assertFalse(e.isConstant());
        e = e("1+null");
        assertEquals(NULL_LITERAL, e);
        e = e("null+1");
        assertEquals(NULL_LITERAL, e);

        e = e("1+2+3+a");
        assertEquals(e("6+a"), e);

        e = e("1+2+3+a+null");
        assertEquals(NULL_LITERAL, e);
    }

    @Test(expected = ArithmeticException.class)
    public void test_fail()
    {
        e("1/0");
    }
}
