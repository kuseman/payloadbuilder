package se.kuseman.payloadbuilder.core.parser;

import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import org.junit.Test;

/** Test of {@link TemplateStringExpression} */
public class TemplateStringExpressionClass extends AParserTest
{
    @Test
    public void test_fold()
    {
        assertEquals(LiteralStringExpression.create("hello world"), e("'he' + `ll${'o' + ' wor' + `ld`}`"));
        assertEquals(LiteralStringExpression.create("mystr123num30"), e("'my' + `str${123 + 'num' + `${10 + 20}`}`"));
    }

    @Test
    public void test_eval() throws Exception
    {
        assertExpression("my 123 string", ofEntries(entry("a", 100), entry("b", 23)), "`my ${a+b} string`");
    }
}
