package se.kuseman.payloadbuilder.api.expression;

/** Definition of an expression visitor. */
public interface IExpressionVisitor<T, C>
{
    T visitChildren(C context, IExpression expression);

    /** Default visit method for unknown expressions */
    default T visit(IExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(IFunctionCallExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(IComparisonExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(IArithmeticBinaryExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralBooleanExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralNullExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralFloatExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralLongExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralDecimalExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralIntegerExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralDoubleExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralStringExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralDateTimeExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(ILiteralDateTimeOffsetExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralArrayExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILiteralObjectExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILogicalBinaryExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILogicalNotExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(INullPredicateExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(IInExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ILikeExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(IArithmeticUnaryExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ICaseExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(IDereferenceExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(INamedExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(INestedExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(ISubscriptExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(IVariableExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(ITemplateStringExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(IColumnExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    // Functions

    default T visit(ICastExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(IAtTimeZoneExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(IDateAddExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(IDatePartExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(IDateDiffExpression expression, C context)
    {
        return visitChildren(context, expression);
    }
}
