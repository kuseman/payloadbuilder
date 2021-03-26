package org.kuse.payloadbuilder.core.parser;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.Type;

/** A table */
public class Table extends TableSource
{
    private final String catalogAlias;

    public Table(String catalogAlias, TableAlias tableAlias, List<Option> options, Token token)
    {
        super(tableAlias, options, token);
        this.catalogAlias = catalogAlias;
    }

    public boolean isTempTable()
    {
        return tableAlias.getType() == Type.TEMPORARY_TABLE;
    }

    @Override
    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    @Override
    public QualifiedName getTable()
    {
        return tableAlias.getTable();
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        return tableAlias.toString();
    }
}
