package se.kuseman.payloadbuilder.core.expression;

import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.IVariableExpression;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;

/** A variable (@var) */
public class VariableExpression implements IVariableExpression, HasAlias
{
    private final String name;
    private final boolean system;

    public VariableExpression(QualifiedName qname)
    {
        this(qname, false);
    }

    public VariableExpression(QualifiedName qname, boolean system)
    {
        this.name = lowerCase(join(qname.getParts(), "."));
        this.system = system;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public boolean isSystem()
    {
        return system;
    }

    @Override
    public Alias getAlias()
    {
        return new Alias(name, "");
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.Any);
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        ExecutionContext ctx = (ExecutionContext) context;
        if (system)
        {
            if ("rowcount".equals(name))
            {
                StatementContext sctx = (StatementContext) context.getStatementContext();
                return ValueVector.literalInt(sctx.getRowCount(), input.getRowCount());
            }
            else if ("version".equals(name))
            {
                return ValueVector.literalString(UTF8String.from(ctx.getVersionString()), input.getRowCount());
            }

            return null;
        }

        ValueVector value = ctx.getVariableValue(name);
        if (value == null)
        {
            return ValueVector.literalNull(ResolvedType.of(Type.Any), input.getRowCount());
        }

        return new ValueVectorAdapter(value)
        {
            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            protected int getRow(int row)
            {
                return 0;
            }
        };
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof VariableExpression)
        {
            VariableExpression that = (VariableExpression) obj;
            return name.equals(that.name)
                    && system == that.system;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "@" + (system ? "@"
                : "")
               + name;
    }
}
