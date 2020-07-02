package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.parser.QualifiedName;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;

/** Domain of a table alias */
public class TableAlias
{
    private final TableAlias parent;
    private final QualifiedName table;
    private final String alias;
    private final List<TableAlias> childAliases = new ArrayList<>();
    private String[] columns;
    // Index of rows of this type in parent row
    private int parentIndex;

    public TableAlias(
            TableAlias parent,
            QualifiedName table,
            String alias)
    {
        this(parent, table, alias, null);
    }

    public TableAlias(
            TableAlias parent,
            QualifiedName table,
            String alias,
            String[] columns)
    {
        this.parent = parent;
        this.table = requireNonNull(table, "table");
        this.alias = requireNonNull(alias, "alias");
        this.columns = columns;

        if (parent != null)
        {
            parent.childAliases.add(this);
            parentIndex = parent.childAliases.size() - 1;
        }
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public String getAlias()
    {
        return alias;
    }

    public TableAlias getParent()
    {
        return parent;
    }

    public List<TableAlias> getChildAliases()
    {
        return childAliases;
    }

    /** Find table alias for provided alias relative to this */
    public TableAlias findAlias(String alias)
    {
        TableAlias current = this;
        while (current != null)
        {
            if (current.alias.equals(alias))
            {
                return current;
            }

            TableAlias child = current.getChildAlias(alias);
            if (child != null)
            {
                return child;
            }

            current = current.parent;
        }

        return null;
    }

    public TableAlias getChildAlias(String alias)
    {
        if (childAliases == null)
        {
            return null;
        }
        for (TableAlias child : childAliases)
        {
            if (Objects.equals(child.alias, alias))
            {
                return child;
            }
        }

        return null;
    }

    public String[] getColumns()
    {
        return columns;
    }

    public void setColumns(String[] columns)
    {
        this.columns = columns;
    }

    public int getParentIndex()
    {
        return parentIndex;
    }

    /** Print alias hierarchy */
    public String printHierarchy(int indent)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtils.repeat(' ', indent * 2));
        sb.append(toString());
        if (columns != null)
        {
            sb.append(" ").append(Arrays.toString(columns));
        }
        sb.append(System.lineSeparator());

        if (childAliases != null)
        {
            childAliases.forEach(a -> sb.append(a.printHierarchy(indent + 1)));
        }

        return sb.toString();
    }

    // NOTE! No hashCode or equals since
    //       this structure is used during build and changes while located in
    //       a Map and hence cannot have other than identity hashCode//equals

    @Override
    public String toString()
    {
        return table + " (" + alias + ")";
    }

    /** Used for test verification only */
    public boolean isEqual(TableAlias other)
    {
        return table.equals(other.table)
            &&
            alias.equals(other.alias)
            &&
            Objects.deepEquals(columns, other.columns)
            &&
            childAliases.size() == other.childAliases.size()
            &&
            IntStream.range(0, childAliases.size()).allMatch(i -> childAliases.get(i).isEqual(other.childAliases.get(i)));
    }

    public static TableAlias of(TableAlias parent, String table, String alias)
    {
        return TableAlias.of(parent, new QualifiedName(null, asList(table)), alias);
    }

    /** Construct a table alias from provided table name */
    public static TableAlias of(TableAlias parent, QualifiedName table, String alias)
    {
        return new TableAlias(parent, table, alias);
    }
}
