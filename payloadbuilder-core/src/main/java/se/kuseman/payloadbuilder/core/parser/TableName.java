package se.kuseman.payloadbuilder.core.parser;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Objects;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Table name */
public class TableName
{
    private final String catalogAlias;
    private final QualifiedName qname;
    private final boolean tempTable;

    TableName(String catalogAlias, QualifiedName qname, boolean tempTable)
    {
        this.catalogAlias = catalogAlias;
        this.qname = qname;
        this.tempTable = tempTable;
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public QualifiedName getQname()
    {
        return qname;
    }

    public boolean isTempTable()
    {
        return tempTable;
    }

    @Override
    public int hashCode()
    {
        return qname.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TableName)
        {
            TableName that = (TableName) obj;
            return Objects.equals(catalogAlias, that.catalogAlias)
                    && qname.equals(that.qname)
                    && tempTable == that.tempTable;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return (!isBlank(catalogAlias) ? catalogAlias
                : "")
                + (tempTable ? "#"
                        : "")
                + qname;
    }
}
