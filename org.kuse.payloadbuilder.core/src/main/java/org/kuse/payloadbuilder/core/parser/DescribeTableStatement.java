package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** DESCRIBE table */
public class DescribeTableStatement extends Statement
{
    private final String catalog;
    private final QualifiedName tableName;

    DescribeTableStatement(String catalog, QualifiedName tableName)
    {
        this.catalog = catalog;
        this.tableName = requireNonNull(tableName, "tableName");
    }
    
    public String getCatalog()
    {
        return catalog;
    }
    
    public QualifiedName getTableName()
    {
        return tableName;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
