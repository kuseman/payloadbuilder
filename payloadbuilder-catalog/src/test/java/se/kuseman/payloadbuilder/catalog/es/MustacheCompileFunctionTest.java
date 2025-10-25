package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link MustacheCompileFunction} */
public class MustacheCompileFunctionTest extends Assert
{
    @Test
    public void test()
    {
        MustacheCompileFunction f = new MustacheCompileFunction();

        assertEquals(Arity.TWO, f.arity());
        assertEquals(ResolvedType.of(Type.String), f.getType(emptyList()));

        IExpression templateArg = mock(IExpression.class);
        IExpression paramsArg = mock(IExpression.class);

        TupleVector input = mock(TupleVector.class);
        IExecutionContext context = mock(IExecutionContext.class);

        when(templateArg.eval(input, context)).thenReturn(VectorTestUtils.vv(ResolvedType.STRING, UTF8String.from("hello {{ param }}"), null));
        when(paramsArg.eval(input, context)).thenReturn(VectorTestUtils.vv(Type.Any, ofEntries(entry("param", "world")), ofEntries(entry("param", "world2"))));

        when(input.getRowCount()).thenReturn(2);

        ValueVector actual = f.evalScalar(context, input, "es", asList(templateArg, paramsArg));
        assertVectorsEquals(VectorTestUtils.vv(Type.String, "hello world", null), actual);
    }
}
