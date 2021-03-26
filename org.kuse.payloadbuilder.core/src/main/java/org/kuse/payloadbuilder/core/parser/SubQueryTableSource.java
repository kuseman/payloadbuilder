package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.operator.TableAlias;

/** Sub query table source */
public class SubQueryTableSource extends TableSource
{
    private final TableSourceJoined from;
    private final Expression where;
    private final List<Expression> groupBy;
    private final List<SortItem> orderBy;
    private final List<SelectItem> selectItems;

    public SubQueryTableSource(
            List<SelectItem> selectItems,
            TableAlias tableAlias,
            List<Option> options,
            TableSourceJoined from,
            Expression where,
            List<Expression> groupBy,
            List<SortItem> orderBy,
            Token token)
    {
        super(tableAlias, options, token);
        this.selectItems = requireNonNull(selectItems, "selectItems");
        this.from = requireNonNull(from, "from");
        this.orderBy = requireNonNull(orderBy, "orderBy");
        this.groupBy = requireNonNull(groupBy, "groupBy");
        this.where = where;

        if (selectItems.size() != 1
            || !(selectItems.get(0) instanceof AsteriskSelectItem)
            || !((AsteriskSelectItem) selectItems.get(0)).isRecursive())
        {
            throw new ParseException("Only a recursive asterisk select (**) is supporte for sub queries", token);
        }
    }

    //    @Override
    //    public TableAlias getTableAlias()
    //    {
    //        return tableSourceJoined.getTableSource().getTableAlias();
    //    }

    public List<SelectItem> getSelectItems()
    {
        return selectItems;
    }

    public TableSourceJoined getFrom()
    {
        return from;
    }

    //    @Override
    //    public String getCatalogAlias()
    //    {
    //        return tableSourceJoined.getTableSource().getCatalogAlias();
    //    }
    //
    //    @Override
    //    public QualifiedName getTable()
    //    {
    //        return tableSourceJoined.getTableSource().getTable();
    //    }

    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }

    public List<Expression> getGroupBy()
    {
        return groupBy;
    }

    public Expression getWhere()
    {
        return where;
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
        sb.append(from).append(System.lineSeparator());
        sb.append(") ").append(tableAlias.getAlias()).append(System.lineSeparator());
        return sb.toString();
    }
}
