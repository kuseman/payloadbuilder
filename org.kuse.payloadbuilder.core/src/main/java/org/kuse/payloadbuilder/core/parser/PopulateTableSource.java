package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;

/** Populating join type. Does not produce
 * tuples but instead populates outer rows with join rows */
public class PopulateTableSource extends TableSource
{
    private final TableSourceJoined tableSourceJoined;
    private final Expression where;
    private final List<Expression> groupBy;
    private final List<SortItem> orderBy;
    
    public PopulateTableSource(
            String alias,
            TableSourceJoined tableSourceJoined,
            Expression where,
            List<Expression> groupBy,
            List<SortItem> orderBy,
            Token token)
    {
        super(alias, token);
        this.tableSourceJoined = requireNonNull(tableSourceJoined, "tableSourceJoined");
        this.orderBy = requireNonNull(orderBy, "orderBy");
        this.groupBy = requireNonNull(groupBy, "groupBy");
        this.where = where;
    }
    
    public TableSourceJoined getTableSourceJoined()
    {
        return tableSourceJoined;
    }
    
    @Override
    public String getCatalogAlias()
    {
        return tableSourceJoined.getTableSource().getCatalogAlias();
    }
    
    @Override
    public QualifiedName getTable()
    {
        return tableSourceJoined.getTableSource().getTable();
    }
    
    @Override
    public List<TableOption> getTableOptions()
    {
        return tableSourceJoined.getTableSource().getTableOptions();
    }
    
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
        sb.append("[").append(System.lineSeparator());
        sb.append(tableSourceJoined).append(System.lineSeparator());
        sb.append("]").append(System.lineSeparator());
        return sb.toString();
    }
}
