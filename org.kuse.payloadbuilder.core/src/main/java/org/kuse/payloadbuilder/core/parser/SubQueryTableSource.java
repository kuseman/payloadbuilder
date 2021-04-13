package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.operator.TableAlias;

/** Sub query table source */
public class SubQueryTableSource extends TableSource
{
    private final Select select;

    public SubQueryTableSource(
            TableAlias tableAlias,
            Select select,
            List<Option> options,
            Token token)
    {
        super(tableAlias, options, token);
        this.select = requireNonNull(select, "select");
    }

    public Select getSelect()
    {
        return select;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(System.lineSeparator());
        sb.append(select).append(System.lineSeparator());
        sb.append(") ").append(tableAlias.getAlias()).append(System.lineSeparator());
        return sb.toString();
    }
}
