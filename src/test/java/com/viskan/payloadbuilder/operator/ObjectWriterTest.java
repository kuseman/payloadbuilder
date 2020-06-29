package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link ObjectWriter} */
public class ObjectWriterTest extends Assert
{
    private final QueryParser parser = new QueryParser();

    @Test
    public void test()
    {
        ObjectWriter w = new ObjectWriter();
        Projection p;

        p = new ExpressionProjection(e("123"));
        assertValue(p, w, 123);

        p = new ObjectProjection(
                asList("a", "b", "c"),
                asList(
                        new ExpressionProjection(e("123")),
                        new ExpressionProjection(e("'string'")),
                        new ExpressionProjection(e("true"))));
        assertValue(p, w, ofEntries(
                entry("a", 123),
                entry("b", "string"),
                entry("c", true)));

        p = new ObjectProjection(
                asList("a", "b", "c"),
                asList(
                        new ExpressionProjection(e("123")),
                        new ExpressionProjection(e("'string'")),
                        new ObjectProjection(asList("a", "b"), asList(
                                new ExpressionProjection(e("1.1")),
                                new ExpressionProjection(e("100"))))));
        assertValue(p, w, ofEntries(
                entry("a", 123),
                entry("b", "string"),
                entry("c", ofEntries(
                        entry("a", 1.1f),
                        entry("b", 100)))));

        p = new ArrayProjection(
                asList(
                        new ExpressionProjection(e("123")),
                        new ExpressionProjection(e("'string'")),
                        new ObjectProjection(asList("a", "b"), asList(
                                new ExpressionProjection(e("1.1")),
                                new ExpressionProjection(e("100"))))),
                null);
        assertValue(p, w, asList(
                123,
                "string",
                ofEntries(
                        entry("a", 1.1f),
                        entry("b", 100))));

        p = new ArrayProjection(
                asList(
                        new ArrayProjection(asList(
                                new ExpressionProjection(e("123")),
                                new ExpressionProjection(e("'string'")),
                                new ObjectProjection(asList("a", "b"), asList(
                                        new ExpressionProjection(e("1.1")),
                                        new ExpressionProjection(e("100"))))),
                                null)),
                null);
        assertValue(p, w, asList(asList(
                123,
                "string",
                ofEntries(
                        entry("a", 1.1f),
                        entry("b", 100)))));

        //        array(object(x.Value * y.Value val, array(x.Value, y.Value, 1337, 665) ar))

        p = new ArrayProjection(
                asList(
                        new ObjectProjection(asList("a", "b"), asList(
                                new ExpressionProjection(e("10")),
                                new ArrayProjection(asList(
                                        new ExpressionProjection(e("'string'")),
                                        new ExpressionProjection(e("1337.666"))), null)))),
                null);

        assertValue(p, w, asList(
                ofEntries(
                        entry("a", 10),
                        entry("b", asList(
                                "string",
                                1337.666f)))));

    }

    private void assertValue(Projection p, ObjectWriter w, Object expected)
    {
        p.writeValue(w, new OperatorContext(), null);
        assertEquals(expected, w.getValue());
    }

    private Expression e(String expression)
    {
        return parser.parseExpression(null, expression);
    }
}
