package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

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
    private static final QualifiedName ROWCOUNT = QualifiedName.of("rowcount");
    private static final QualifiedName VERSION = QualifiedName.of("version");

    private final QualifiedName name;
    private final boolean system;

    public VariableExpression(QualifiedName qname)
    {
        this(qname, false);
    }

    public VariableExpression(QualifiedName qname, boolean system)
    {
        this.name = requireNonNull(qname, "qname").toLowerCase();
        this.system = system;
    }

    @Override
    public QualifiedName getName()
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
        return new Alias(name.toString(), "");
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
        return eval(input, ValueVector.range(0, input.getRowCount()), context);
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        ExecutionContext ctx = (ExecutionContext) context;
        if (system)
        {
            if (ROWCOUNT.equalsIgnoreCase(name))
            {
                StatementContext sctx = (StatementContext) context.getStatementContext();
                return ValueVector.literalInt(sctx.getRowCount(), selection.size());
            }
            else if (VERSION.equalsIgnoreCase(name))
            {
                return ValueVector.literalString(UTF8String.from(ctx.getVersionString()), selection.size());
            }

            return null;
        }

        ValueVector value = ctx.getVariableValue(name);
        if (value == null)
        {
            return ValueVector.literalNull(ResolvedType.of(Type.Any), selection.size());
        }

        return new ValueVectorAdapter(value)
        {
            @Override
            public int size()
            {
                return selection.size();
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
