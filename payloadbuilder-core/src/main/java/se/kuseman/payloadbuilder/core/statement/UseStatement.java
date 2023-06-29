package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;

/** Use statement */
public class UseStatement extends Statement
{
    private final QualifiedName qname;
    private final IExpression expression;

    public UseStatement(QualifiedName qname, IExpression expression)
    {
        this.qname = requireNonNull(qname, "qname");
        this.expression = expression;
    }

    public QualifiedName getQname()
    {
        return qname;
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

    /** Execute use statement and populate session with values */
    public void execute(ExecutionContext context)
    {
        // Change of default catalog
        if (expression == null)
        {
            context.getSession()
                    .setDefaultCatalogAlias(qname.getFirst());
        }
        else
        {
            String catalogAlias = qname.getFirst();
            QualifiedName property = qname.extract(1);

            String key = join(property.getParts(), ".");
            ValueVector value = expression.eval(TupleVector.CONSTANT, context);
            context.getSession()
                    .setCatalogProperty(catalogAlias, key, value);
        }
    }
}
