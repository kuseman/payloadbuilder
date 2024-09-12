package se.kuseman.payloadbuilder.test;

import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;

/** Helper class for mocking {@link IAnalyzePair}'s */
public class IPredicateMock
{
    public static IPredicate neq(String column, Object value)
    {
        return comparison(QualifiedName.of(column), value, IComparisonExpression.Type.NOT_EQUAL);
    }

    public static IPredicate eq(String column, Object value)
    {
        return comparison(QualifiedName.of(column), value, IComparisonExpression.Type.EQUAL);
    }

    public static IPredicate gt(String column, Object value)
    {
        return comparison(QualifiedName.of(column), value, IComparisonExpression.Type.GREATER_THAN);
    }

    public static IPredicate gte(String column, Object value)
    {
        return comparison(QualifiedName.of(column), value, IComparisonExpression.Type.GREATER_THAN_EQUAL);
    }

    public static IPredicate lt(String column, Object value)
    {
        return comparison(QualifiedName.of(column), value, IComparisonExpression.Type.LESS_THAN);
    }

    public static IPredicate lte(String column, Object value)
    {
        return comparison(QualifiedName.of(column), value, IComparisonExpression.Type.LESS_THAN_EQUAL);
    }

    /** Mock a comparison pair */
    public static IPredicate comparison(QualifiedName column, Object value, IComparisonExpression.Type type)
    {
        IPredicate pair = mock(IPredicate.class);
        when(pair.getQualifiedColumn()).thenReturn(column);
        when(pair.getType()).thenReturn(IPredicate.Type.COMPARISION);
        when(pair.getComparisonType()).thenReturn(type);

        IExpression comparisonExpression = expression(value);

        when(pair.getComparisonExpression()).thenReturn(comparisonExpression);
        return pair;
    }

    public static IPredicate in(String column, List<Object> values)
    {
        return inPair(column, false, values);
    }

    public static IPredicate notIn(String column, List<Object> values)
    {
        return inPair(column, true, values);
    }

    /** Mock a {@link INullPredicateExpression} predicate. */
    public static IPredicate _null(String column, boolean not)
    {
        IPredicate pair = mock(IPredicate.class);
        when(pair.getQualifiedColumn()).thenReturn(QualifiedName.of(column));
        when(pair.getType()).thenReturn(IPredicate.Type.NULL);

        INullPredicateExpression expression = mock(INullPredicateExpression.class);

        when(pair.getNullPredicateExpression()).thenReturn(expression);
        when(expression.isNot()).thenReturn(not);
        return pair;
    }

    /** Mock an IN pair */
    public static IPredicate inPair(String column, boolean not, List<Object> values)
    {
        IPredicate pair = mock(IPredicate.class);
        when(pair.getQualifiedColumn()).thenReturn(QualifiedName.of(column));
        when(pair.getType()).thenReturn(IPredicate.Type.IN);

        IInExpression inExpression = mock(IInExpression.class);

        when(pair.getInExpression()).thenReturn(inExpression);

        when(inExpression.isNot()).thenReturn(not);

        List<IExpression> arguments = values.stream()
                .map(v -> expression(v))
                .collect(toList());

        when(inExpression.getArguments()).then(new Answer<List<IExpression>>()
        {
            @Override
            public List<IExpression> answer(InvocationOnMock invocation) throws Throwable
            {
                return arguments;
            }
        });

        return pair;
    }

    public static IPredicate like(String column, String likePattern)
    {
        return likePair(column, false, likePattern);
    }

    public static IPredicate notLike(String column, String likePattern)
    {
        return likePair(column, true, likePattern);
    }

    /** Mock a LIKE pair */
    public static IPredicate likePair(String column, boolean not, String likePattern)
    {
        IPredicate pair = mock(IPredicate.class);
        when(pair.getQualifiedColumn()).thenReturn(QualifiedName.of(column));
        when(pair.getType()).thenReturn(IPredicate.Type.LIKE);

        ILikeExpression likeExpression = mock(ILikeExpression.class);
        when(pair.getLikeExpression()).thenReturn(likeExpression);
        when(likeExpression.isNot()).thenReturn(not);

        IExpression pattern = expression(likePattern);

        when(likeExpression.getPatternExpression()).thenReturn(pattern);

        return pair;
    }

    /** Mock FUNCTION_CALL with arguments */
    public static IPredicate function(String catalogAlias, String functionName, List<Object> arguments)
    {
        IPredicate predicate = mock(IPredicate.class);
        when(predicate.getType()).thenReturn(IPredicate.Type.FUNCTION_CALL);

        IFunctionCallExpression functionExpression = mock(IFunctionCallExpression.class);
        ScalarFunctionInfo functionInfo = mock(ScalarFunctionInfo.class);
        when(functionInfo.getName()).thenReturn(functionName);
        when(predicate.getFunctionCallExpression()).thenReturn(functionExpression);
        when(functionExpression.getCatalogAlias()).thenReturn(catalogAlias);
        when(functionExpression.getFunctionInfo()).thenReturn(functionInfo);

        List<IExpression> functionArguments = arguments.stream()
                .map(v -> v instanceof IExpression ? (IExpression) v
                        : expression(v))
                .collect(toList());

        when(functionExpression.getArguments()).then(new Answer<List<IExpression>>()
        {
            @Override
            public List<IExpression> answer(InvocationOnMock invocation) throws Throwable
            {
                return functionArguments;
            }
        });
        return predicate;
    }

    /** Mock an expression */
    public static IExpression expression(Object value)
    {
        IExpression mock = mock(IExpression.class);
        ValueVector vector;
        if (value == null)
        {
            vector = ValueVector.literalNull(ResolvedType.of(Type.Any), 1);
        }
        else
        {
            vector = ValueVector.literalAny(1, value);
        }
        when(mock.eval(any(IExecutionContext.class))).thenReturn(vector);
        when(mock.eval(any(TupleVector.class), any(IExecutionContext.class))).thenReturn(vector);
        return mock;
    }
}
