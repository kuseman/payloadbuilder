package se.kuseman.payloadbuilder.test;

import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import se.kuseman.payloadbuilder.api.catalog.IAnalyzePair;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.IQualifiedFunctionCallExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Helper class for mocking {@link IAnalyzePair}'s */
public class IAnalyzePairMock
{
    public static IAnalyzePair neq(String column, Object value)
    {
        return comparison(column, value, IComparisonExpression.Type.NOT_EQUAL);
    }

    public static IAnalyzePair eq(String column, Object value)
    {
        return comparison(column, value, IComparisonExpression.Type.EQUAL);
    }

    public static IAnalyzePair gt(String column, Object value)
    {
        return comparison(column, value, IComparisonExpression.Type.GREATER_THAN);
    }

    public static IAnalyzePair gte(String column, Object value)
    {
        return comparison(column, value, IComparisonExpression.Type.GREATER_THAN_EQUAL);
    }

    public static IAnalyzePair lt(String column, Object value)
    {
        return comparison(column, value, IComparisonExpression.Type.LESS_THAN);
    }

    public static IAnalyzePair lte(String column, Object value)
    {
        return comparison(column, value, IComparisonExpression.Type.LESS_THAN_EQUAL);
    }

    /** Mock a comparison pair */
    public static IAnalyzePair comparison(String column, Object value, IComparisonExpression.Type type)
    {
        IAnalyzePair pair = mock(IAnalyzePair.class);
        when(pair.getColumn(anyString())).thenReturn(column);
        when(pair.getType()).thenReturn(IAnalyzePair.Type.COMPARISION);
        when(pair.getComparisonType()).thenReturn(type);
        when(pair.getComparisonExpression(anyString())).thenReturn(ctx -> value);
        return pair;
    }

    public static IAnalyzePair in(String column, List<Object> values)
    {
        return inPair(column, false, values);
    }

    public static IAnalyzePair notIn(String column, List<Object> values)
    {
        return inPair(column, true, values);
    }

    /** Mock an IN pair */
    public static IAnalyzePair inPair(String column, boolean not, List<Object> values)
    {
        IAnalyzePair pair = mock(IAnalyzePair.class);
        when(pair.getColumn(anyString())).thenReturn(column);
        when(pair.getType()).thenReturn(IAnalyzePair.Type.IN);

        IInExpression inExpression = mock(IInExpression.class);

        when(pair.getInExpression(anyString())).thenReturn(inExpression);

        when(inExpression.isNot()).thenReturn(not);

        List<? extends IExpression> arguments = values.stream()
                .map(v -> expression(v))
                .collect(toList());

        when(inExpression.getArguments()).then(new Answer<List<? extends IExpression>>()
        {
            @Override
            public List<? extends IExpression> answer(InvocationOnMock invocation) throws Throwable
            {
                return arguments;
            }
        });

        return pair;
    }

    public static IAnalyzePair like(String column, String likePattern)
    {
        return likePair(column, false, likePattern);
    }

    public static IAnalyzePair notLike(String column, String likePattern)
    {
        return likePair(column, true, likePattern);
    }

    /** Mock a LIKE pair */
    public static IAnalyzePair likePair(String column, boolean not, String likePattern)
    {
        IAnalyzePair pair = mock(IAnalyzePair.class);
        when(pair.getColumn(anyString())).thenReturn(column);
        when(pair.getType()).thenReturn(IAnalyzePair.Type.LIKE);

        ILikeExpression likeExpression = mock(ILikeExpression.class);
        when(pair.getLikeExpression(anyString())).thenReturn(likeExpression);
        when(likeExpression.isNot()).thenReturn(not);
        when(likeExpression.getPatternExpression()).thenReturn(ctx -> likePattern);

        return pair;
    }

    /** Mock UNDEFINED scalar function pair */
    public static IAnalyzePair function(ScalarFunctionInfo functioInfo, List<Object> arguments)
    {
        IAnalyzePair pair = mock(IAnalyzePair.class);
        when(pair.getType()).thenReturn(IAnalyzePair.Type.UNDEFINED);

        IQualifiedFunctionCallExpression functionExpression = mock(IQualifiedFunctionCallExpression.class);
        when(pair.getUndefinedValueExpression(ArgumentMatchers.eq(IQualifiedFunctionCallExpression.class))).thenReturn(functionExpression);
        when(functionExpression.getFunctionInfo()).thenReturn(functioInfo);

        List<? extends IExpression> functionArguments = arguments.stream()
                .map(v -> expression(v))
                .collect(toList());

        when(functionExpression.getArguments()).then(new Answer<List<? extends IExpression>>()
        {
            @Override
            public List<? extends IExpression> answer(InvocationOnMock invocation) throws Throwable
            {
                return functionArguments;
            }
        });
        return pair;
    }

    /** Mock an expression */
    public static IExpression expression(Object value)
    {
        IExpression mock = mock(IExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(value);
        return mock;
    }
}
