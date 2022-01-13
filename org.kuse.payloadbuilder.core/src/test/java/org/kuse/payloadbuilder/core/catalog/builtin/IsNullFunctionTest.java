package org.kuse.payloadbuilder.core.catalog.builtin;

import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.NoOpTuple;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.AParserTest;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.mockito.Mockito;

/** Test of {@link IsNullFunction} */
public class IsNullFunctionTest extends AParserTest
{
    @Test
    public void test_dataType()
    {
        assertEquals(DataType.ANY, e("isnull(a, b)").getDataType());
        assertEquals(DataType.ANY, e("isnull(a, 10)").getDataType());
        assertEquals(DataType.INT, e("isnull(20, 10)").getDataType());
    }

    @Test
    public void test_codeGen()
    {
        Predicate<ExecutionContext> p = CODE_GENERATOR.generatePredicate(e("isnull(a, true)"));
        context.getStatementContext().setTuple(NoOpTuple.NO_OP);
        assertTrue(p.test(context));

        p = CODE_GENERATOR.generatePredicate(e("isnull(10, 20) = 10"));
        assertTrue(p.test(context));

        p = CODE_GENERATOR.generatePredicate(e("isnull(null, 20l) = 20l"));
        assertTrue(p.test(context));

        // Set data type of reference
        Expression e = e("isnull(a, 20l) = 20l", Mockito.mock(Tuple.class), ofEntries(entry("a", Pair.of(null, DataType.INT))));

        p = CODE_GENERATOR.generatePredicate(e);
        assertTrue(p.test(context));
    }
}
