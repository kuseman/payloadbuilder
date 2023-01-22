package se.kuseman.payloadbuilder.core.expression;

import static java.util.stream.Collectors.toList;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IArithmeticUnaryExpression;
import se.kuseman.payloadbuilder.api.expression.IAtTimeZoneExpression;
import se.kuseman.payloadbuilder.api.expression.ICaseExpression;
import se.kuseman.payloadbuilder.api.expression.ICastExpression;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IDateAddExpression;
import se.kuseman.payloadbuilder.api.expression.IDateDiffExpression;
import se.kuseman.payloadbuilder.api.expression.IDatePartExpression;
import se.kuseman.payloadbuilder.api.expression.IDereferenceExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralBooleanExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDoubleExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralFloatExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralIntegerExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralLongExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralNullExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralStringExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalNotExpression;
import se.kuseman.payloadbuilder.api.expression.INamedExpression;
import se.kuseman.payloadbuilder.api.expression.INestedExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;
import se.kuseman.payloadbuilder.api.expression.ISubscriptExpression;
import se.kuseman.payloadbuilder.api.expression.ITemplateStringExpression;
import se.kuseman.payloadbuilder.api.expression.IVariableExpression;

/**
 * Base class for expression rewriting. Rewrites the expression tree during traversal. Enabling clients for manipulate certain expression along the way
 */
public abstract class ARewriteExpressionVisitor<C> extends AExpressionVisitor<IExpression, C>
{
    private List<IExpression> visit(List<IExpression> expressions, C context)
    {
        return expressions.stream()
                .map(e -> e.accept(this, context))
                .collect(toList());
    }

    @Override
    public IExpression visit(IExpression expression, C context)
    {
        if (expression == null)
        {
            return null;
        }
        return expression.accept(this, context);
    }

    @Override
    public IExpression visit(AsteriskExpression expression, C context)
    {
        // Asterisk has no state
        return expression;
    };

    @Override
    public IExpression visit(UnresolvedColumnExpression expression, C context)
    {
        // Column doesn't have any child expressions
        return expression;
    };

    @Override
    public IExpression visit(AssignmentExpression expression, C context)
    {
        return new AssignmentExpression(expression.getExpression()
                .accept(this, context), QualifiedName.of(expression.getVariable()));
    }

    @Override
    public IExpression visit(UnresolvedFunctionCallExpression expression, C context)
    {
        return new UnresolvedFunctionCallExpression(expression.getCatalogAlias(), expression.getName(), expression.getAggregateMode(), visit(expression.getChildren(), context), expression.getToken());
    };

    @Override
    public IExpression visit(IFunctionCallExpression expression, C context)
    {
        return new FunctionCallExpression(expression.getCatalogAlias(), expression.getFunctionInfo(), expression.getAggregateMode(), visit(expression.getArguments(), context));
    }

    @Override
    public IExpression visit(IComparisonExpression expression, C context)
    {
        return new ComparisonExpression(expression.getComparisonType(), visit(expression.getLeft(), context), visit(expression.getRight(), context));
    };

    @Override
    public IExpression visit(IArithmeticBinaryExpression expression, C context)
    {
        return new ArithmeticBinaryExpression(expression.getArithmeticType(), visit(expression.getLeft(), context), visit(expression.getRight(), context));
    };

    @Override
    public IExpression visit(ILiteralBooleanExpression expression, C context)
    {
        return expression;
    };

    @Override
    public IExpression visit(ILiteralNullExpression expression, C context)
    {
        return expression;
    };

    @Override
    public IExpression visit(ILiteralFloatExpression expression, C context)
    {
        return expression;
    };

    @Override
    public IExpression visit(ILiteralLongExpression expression, C context)
    {
        return expression;
    };

    @Override
    public IExpression visit(ILiteralIntegerExpression expression, C context)
    {
        return expression;
    };

    @Override
    public IExpression visit(ILiteralDoubleExpression expression, C context)
    {
        return expression;
    };

    @Override
    public IExpression visit(ILiteralStringExpression expression, C context)
    {
        return expression;
    };

    @Override
    public IExpression visit(ILogicalBinaryExpression expression, C context)
    {
        return new LogicalBinaryExpression(expression.getLogicalType(), visit(expression.getLeft(), context), visit(expression.getRight(), context));
    };

    @Override
    public IExpression visit(ILogicalNotExpression expression, C context)
    {
        return new LogicalNotExpression(visit(expression.getExpression(), context));
    };

    @Override
    public IExpression visit(INullPredicateExpression expression, C context)
    {
        return new NullPredicateExpression(visit(expression.getExpression(), context), expression.isNot());
    };

    @Override
    public IExpression visit(IInExpression expression, C context)
    {
        return new InExpression(visit(expression.getExpression(), context), visit(expression.getArguments(), context), expression.isNot());
    };

    @Override
    public IExpression visit(ILikeExpression expression, C context)
    {
        return new LikeExpression(visit(expression.getExpression(), context), visit(expression.getPatternExpression(), context), expression.isNot(), null);
    };

    @Override
    public IExpression visit(IArithmeticUnaryExpression expression, C context)
    {
        return new ArithmeticUnaryExpression(expression.getArithmeticType(), visit(expression.getExpression(), context));
    };

    @Override
    public IExpression visit(ICaseExpression expression, C context)
    {
        List<ICaseExpression.WhenClause> whenClauses = expression.getWhenClauses()
                .stream()
                .map(w -> new ICaseExpression.WhenClause(visit(w.getCondition(), context), visit(w.getResult(), context)))
                .collect(toList());

        return new CaseExpression(whenClauses, visit(expression.getElseExpression(), context));
    };

    @Override
    public IExpression visit(IDereferenceExpression expression, C context)
    {
        if (((DereferenceExpression) expression).isResolved())
        {
            return new DereferenceExpression(visit(expression.getExpression(), context), expression.getRight(), ((DereferenceExpression) expression).getOrdinal(), expression.getType());
        }
        return new DereferenceExpression(visit(expression.getExpression(), context), expression.getRight());
    };

    @Override
    public IExpression visit(INamedExpression expression, C context)
    {
        return new NamedExpression(expression.getName(), visit(expression.getExpression(), context));
    };

    @Override
    public IExpression visit(INestedExpression expression, C context)
    {
        return new NestedExpression(visit(expression.getExpression(), context));
    };

    @Override
    public IExpression visit(ISubscriptExpression expression, C context)
    {
        return new SubscriptExpression(visit(expression.getValue(), context), visit(expression.getSubscript(), context));
    };

    @Override
    public IExpression visit(IVariableExpression expression, C context)
    {
        return expression;
    }

    @Override
    public IExpression visit(SubQueryExpression expression, C context)
    {
        // Visiting the plan of sub query expression is up to clients
        return expression;
    }

    @Override
    public IExpression visit(LambdaExpression expression, C context)
    {
        return new LambdaExpression(expression.getIdentifiers(), visit(expression.getExpression(), context), expression.getLambdaIds());
    }

    @Override
    public IExpression visit(ITemplateStringExpression expression, C context)
    {
        return new TemplateStringExpression(visit(expression.getExpressions(), context));
    }

    @Override
    public IExpression visit(AliasExpression expression, C context)
    {
        return new AliasExpression(visit(expression.getExpression(), context), expression.getAliasString(), expression.getOutputAlias(), expression.isInternal());
    }

    @Override
    public IExpression visit(IColumnExpression expression, C context)
    {
        return expression;
    }

    @Override
    public IExpression visit(AggregateWrapperExpression expression, C context)
    {
        return new AggregateWrapperExpression(visit(expression.getExpression(), context), expression.isSingleValue(), expression.isInternal());
    }

    @Override
    public IExpression visit(ICastExpression expression, C context)
    {
        return new CastExpression(visit(expression.getExpression(), context), expression.getType());
    }

    @Override
    public IExpression visit(IAtTimeZoneExpression expression, C context)
    {
        return new AtTimeZoneExpression(visit(expression.getExpression(), context), visit(expression.getTimeZone(), context));
    }

    @Override
    public IExpression visit(IDateAddExpression expression, C context)
    {
        return new DateAddExpression(expression.getPart(), visit(expression.getNumber(), context), visit(expression.getExpression(), context));
    }

    @Override
    public IExpression visit(IDateDiffExpression expression, C context)
    {
        return new DateDiffExpression(expression.getPart(), visit(expression.getStart(), context), visit(expression.getEnd(), context));
    }

    @Override
    public IExpression visit(IDatePartExpression expression, C context)
    {
        return new DatePartExpression(expression.getPart(), visit(expression.getExpression(), context));
    }
}
