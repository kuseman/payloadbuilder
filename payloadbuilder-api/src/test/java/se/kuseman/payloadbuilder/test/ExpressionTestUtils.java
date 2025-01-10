package se.kuseman.payloadbuilder.test;

import static java.util.Objects.requireNonNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IDereferenceExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralBooleanExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDateTimeExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDateTimeOffsetExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDecimalExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDoubleExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralFloatExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralIntegerExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralLongExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralNullExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralStringExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalNotExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;
import se.kuseman.payloadbuilder.api.expression.IVariableExpression;

/** Test utils when working with {@link IExpression}'s */
public class ExpressionTestUtils
{
    /** Create a {@ILiteralStringExpression} with provided value */
    public static ILiteralStringExpression createStringExpression(String value)
    {
        requireNonNull(value);
        ILiteralStringExpression mock = Mockito.mock(ILiteralStringExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.String, value));
        when(mock.getValue()).thenReturn(UTF8String.from(value));
        when(mock.toString()).thenReturn(value);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.String));
        return mock;
    }

    /** Create a {@ILiteralIntegerExpression} with provided value */
    public static ILiteralIntegerExpression createIntegerExpression(int value)
    {
        ILiteralIntegerExpression mock = Mockito.mock(ILiteralIntegerExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.Int, value));
        when(mock.getValue()).thenReturn(value);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Int));
        return mock;
    }

    /** Create a {@ILiteralLongExpression} with provided value */
    public static ILiteralLongExpression createLongExpression(long value)
    {
        ILiteralLongExpression mock = Mockito.mock(ILiteralLongExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.Long, value));
        when(mock.getValue()).thenReturn(value);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Long));
        return mock;
    }

    /** Create a {@ILiteralFloatExpression} with provided value */
    public static ILiteralFloatExpression createFloatExpression(float value)
    {
        ILiteralFloatExpression mock = Mockito.mock(ILiteralFloatExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.Float, value));
        when(mock.getValue()).thenReturn(value);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Float));
        return mock;
    }

    /** Create a {@ILiteralDoubleExpression} with provided value */
    public static ILiteralDoubleExpression createDoubleExpression(double value)
    {
        ILiteralDoubleExpression mock = Mockito.mock(ILiteralDoubleExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.Double, value));
        when(mock.getValue()).thenReturn(value);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Double));
        return mock;
    }

    /** Create a {@ILiteralBooleanExpression} with provided value */
    public static ILiteralBooleanExpression createBooleanExpression(boolean value)
    {
        ILiteralBooleanExpression mock = Mockito.mock(ILiteralBooleanExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.Boolean, value));
        when(mock.getValue()).thenReturn(value);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Boolean));
        return mock;
    }

    /** Create a {@ILiteralDecimalExpression} with provided value */
    public static ILiteralDecimalExpression createDecimalExpression(Decimal value)
    {
        ILiteralDecimalExpression mock = Mockito.mock(ILiteralDecimalExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.Decimal, value));
        when(mock.getValue()).thenReturn(value);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Decimal));
        return mock;
    }

    /** Create a {@ILiteralDateTimeExpression} with provided value */
    public static ILiteralDateTimeExpression createDateTimeExpression(EpochDateTime value)
    {
        ILiteralDateTimeExpression mock = Mockito.mock(ILiteralDateTimeExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.DateTime, value));
        when(mock.getValue()).thenReturn(value);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.DateTime));
        return mock;
    }

    /** Create a {@ILiteralDateTimeOffsetExpression} with provided value */
    public static ILiteralDateTimeOffsetExpression createDateTimeOffsetExpression(EpochDateTimeOffset value)
    {
        ILiteralDateTimeOffsetExpression mock = Mockito.mock(ILiteralDateTimeOffsetExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.DateTimeOffset, value));
        when(mock.getValue()).thenReturn(value);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.DateTimeOffset));
        return mock;
    }

    /** Create a null expression */
    public static ILiteralNullExpression createNullExpression()
    {
        ILiteralNullExpression mock = Mockito.mock(ILiteralNullExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.Any, new Object[] { null }));
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Any));
        return mock;
    }

    /** Create a variable expression */
    public static IVariableExpression var(String name, Object value)
    {
        IVariableExpression mock = Mockito.mock(IVariableExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.Any, new Object[] { value }));
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Any));
        return mock;
    }

    /** Create a null predicate expression */
    public static INullPredicateExpression isnull(IExpression expression)
    {
        INullPredicateExpression mock = Mockito.mock(INullPredicateExpression.class);
        when(mock.getExpression()).thenReturn(expression);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.isNot()).thenReturn(false);
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Boolean));
        return mock;
    }

    /** Create a not null predicate expression */
    public static INullPredicateExpression isnotnull(IExpression expression)
    {
        INullPredicateExpression mock = Mockito.mock(INullPredicateExpression.class);
        when(mock.getExpression()).thenReturn(expression);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.isNot()).thenReturn(true);
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Boolean));
        return mock;
    }

    /** Create a comparison epxression */
    public static IComparisonExpression ce(IComparisonExpression.Type type, IExpression left, IExpression right)
    {
        IComparisonExpression mock = Mockito.mock(IComparisonExpression.class);
        when(mock.getLeft()).thenReturn(left);
        when(mock.getRight()).thenReturn(right);
        when(mock.getComparisonType()).thenReturn(type);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Boolean));
        return mock;
    }

    /** Create an and epxression */
    public static ILogicalBinaryExpression and(IExpression left, IExpression right)
    {
        ILogicalBinaryExpression mock = Mockito.mock(ILogicalBinaryExpression.class);
        when(mock.getLeft()).thenReturn(left);
        when(mock.getRight()).thenReturn(right);
        when(mock.getLogicalType()).thenReturn(ILogicalBinaryExpression.Type.AND);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Boolean));
        return mock;
    }

    /** Create a or epxression */
    public static ILogicalBinaryExpression or(IExpression left, IExpression right)
    {
        ILogicalBinaryExpression mock = Mockito.mock(ILogicalBinaryExpression.class);
        when(mock.getLeft()).thenReturn(left);
        when(mock.getRight()).thenReturn(right);
        when(mock.getLogicalType()).thenReturn(ILogicalBinaryExpression.Type.OR);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Boolean));
        return mock;
    }

    /** Create a not epxression */
    public static ILogicalNotExpression not(IExpression expression)
    {
        ILogicalNotExpression mock = Mockito.mock(ILogicalNotExpression.class);
        when(mock.getExpression()).thenReturn(expression);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Boolean));
        return mock;
    }

    /** Create an arithmetic epxression */
    public static IArithmeticBinaryExpression arit(IArithmeticBinaryExpression.Type type, IExpression left, IExpression right)
    {
        IArithmeticBinaryExpression mock = Mockito.mock(IArithmeticBinaryExpression.class);
        when(mock.getLeft()).thenReturn(left);
        when(mock.getRight()).thenReturn(right);
        when(mock.getArithmeticType()).thenReturn(type);
        when(mock.accept(any(), any())).thenCallRealMethod();
        return mock;
    }

    /** Create a dereference epxression */
    public static IDereferenceExpression deref(IExpression left, String right)
    {
        IDereferenceExpression mock = Mockito.mock(IDereferenceExpression.class);
        when(mock.getExpression()).thenReturn(left);
        when(mock.getRight()).thenReturn(right);
        when(mock.getQualifiedColumn()).thenCallRealMethod();
        when(mock.accept(any(), any())).thenCallRealMethod();
        return mock;
    }

    /** Create an in epxression */
    public static IInExpression in(IExpression expression, List<IExpression> arguments)
    {
        IInExpression mock = Mockito.mock(IInExpression.class);
        when(mock.getExpression()).thenReturn(expression);
        when(mock.getArguments()).thenReturn(arguments);
        when(mock.isNot()).thenReturn(false);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Boolean));
        return mock;
    }

    /** Create a not in epxression */
    public static IInExpression notIn(IExpression expression, List<IExpression> arguments)
    {
        IInExpression mock = Mockito.mock(IInExpression.class);
        when(mock.getExpression()).thenReturn(expression);
        when(mock.getArguments()).thenReturn(arguments);
        when(mock.isNot()).thenReturn(true);
        when(mock.accept(any(), any())).thenCallRealMethod();
        when(mock.getType()).thenReturn(ResolvedType.of(Type.Boolean));
        return mock;
    }

    /** Create a column epxression */
    public static IColumnExpression col(String column)
    {
        IColumnExpression mock = Mockito.mock(IColumnExpression.class);
        when(mock.getColumn()).thenReturn(column);
        when(mock.getQualifiedColumn()).thenReturn(QualifiedName.of(column));
        when(mock.accept(any(), any())).thenCallRealMethod();
        return mock;
    }

    /** Create a expression form a qualifier IColumnExpression or a nested IDerefernceExpression */
    public static IExpression col(QualifiedName column)
    {
        if (column.size() == 1)
        {
            return col(column.getFirst());
        }

        IExpression result = null;
        for (int i = 0; i < column.size(); i++)
        {
            if (result == null)
            {
                result = col(column.getParts()
                        .get(i));
            }
            else
            {
                result = deref(result, column.getParts()
                        .get(i));
            }
        }

        return result;
    }

    /** Mock a function call with arguments as expressions. */
    public static IFunctionCallExpression function(String catalogAlias, String functionName, List<IExpression> arguments, Object result)
    {
        IFunctionCallExpression functionExpression = mock(IFunctionCallExpression.class);
        doCallRealMethod().when(functionExpression)
                .accept(any(), any());
        ScalarFunctionInfo functionInfo = mock(ScalarFunctionInfo.class);
        when(functionInfo.getName()).thenReturn(functionName);
        when(functionExpression.getCatalogAlias()).thenReturn(catalogAlias);
        when(functionExpression.getFunctionInfo()).thenReturn(functionInfo);
        when(functionExpression.getArguments()).thenReturn(arguments);
        when(functionExpression.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.Any, result));
        return functionExpression;
    }
}
