package se.kuseman.payloadbuilder.core.parser;

import java.util.List;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;

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
        return getTableAlias().getType() == TableAlias.Type.TEMPORARY_TABLE;
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
