package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;

/** Set statement */
public class SetStatement extends Statement
{
    private final QualifiedName name;
    private final IExpression expression;
    private final boolean systemProperty;

    public SetStatement(QualifiedName name, IExpression expression, boolean systemProperty)
    {
        this.name = requireNonNull(name, "name");
        this.expression = requireNonNull(expression, "expression");
        this.systemProperty = systemProperty;
    }

    public boolean isSystemProperty()
    {
        return systemProperty;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    /** Execute statement against provided context. */
    public void execute(IExecutionContext context)
    {
        ValueVector value = expression.eval(TupleVector.CONSTANT, context);
        if (isSystemProperty())
        {
            ((QuerySession) context.getSession()).setSystemProperty(getName(), value);
        }
        else
        {
            ((ExecutionContext) context).setVariable(getName(), value);
        }
    }
}
