package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.function.Predicate;

import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.NoOpTuple;
import org.kuse.payloadbuilder.core.parser.AParserTest;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedFunctionCallExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;

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

    @SuppressWarnings("deprecation")
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
        ComparisonExpression ce = (ComparisonExpression) e("isnull(a, 20l) = 20l");
        QualifiedFunctionCallExpression qfe = (QualifiedFunctionCallExpression) ce.getLeft();
        ((QualifiedReferenceExpression) qfe.getArguments().get(0)).setResolvePaths(asList(new ResolvePath(-1, 0, asList("a"), -1, new int[0], DataType.LONG)));

        p = CODE_GENERATOR.generatePredicate(ce);
        assertTrue(p.test(context));
    }
}
