package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Domain of a table alias */
public class TableAlias
{
    private static final String[] NOT_SET = ArrayUtils.EMPTY_STRING_ARRAY;
    
    private final TableAlias parent;
    private final QualifiedName table;
    private final String alias;
    private final List<TableAlias> childAliases = new ArrayList<>();
    /** Index of this alias in parent child aliases */
    private final int parentIndex;
    private String[] columns = NOT_SET;
    /** Is all columns wanted for this table alias. */
    private boolean asteriskColumns;
    private String rootPath;

//    TableAlias(
//            TableAlias parent,
//            QualifiedName table,
//            String alias)
//    {
//        this(parent, table, alias, NOT_SET, null);
//    }
//
//    TableAlias(
//            TableAlias parent,
//            QualifiedName table,
//            String alias,
//            Token token)
//    {
//        this(parent, table, alias, NOT_SET, token);
//    }
//
//    TableAlias(
//            TableAlias parent,
//            QualifiedName table,
//            String alias,
//            String[] columns)
//    {
//        this(parent, table, alias, columns, null);
//    }

    TableAlias(
            TableAlias parent,
            QualifiedName table,
            String alias,
            String[] columns,
            boolean asteriskColumns,
            Token token)
    {
        this.parent = parent;
        this.table = requireNonNull(table, "table");
        this.alias = requireNonNull(alias, "alias");
        this.columns = requireNonNull(columns, "columns");
        this.asteriskColumns = asteriskColumns;
        
        int parentIndex = -1;
        if (parent != null)
        {
            TableAlias temp = parent;
            while (temp != null)
            {
                if (alias.equals(temp.alias))
                {
                    throw new ParseException("Alias " + alias + " already exists in scope.", token);
                }
                if (temp.getChildAlias(alias) != null)
                {
                    throw new ParseException("Alias " + alias + " already exists in scope.", token);
                }
                temp = temp.parent;
            }

            parent.childAliases.add(this);
            parentIndex = parent.childAliases.size() - 1;
        }
        this.parentIndex = parentIndex;
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

    /** Get root path from alias */
    public String getRootPath()
    {
        if (rootPath == null)
        {
            StringBuilder sb = new StringBuilder();
            TableAlias current = this;
            while (current != null)
            {
                sb.insert(0, current.alias + "/");
                current = current.parent;
            }
            rootPath = sb.toString();
        }
        return rootPath;
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
        int size = childAliases.size();
        for (int i = 0; i < size; i++)
        {
            TableAlias child = childAliases.get(i);
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

    void setColumns(String[] columns)
    {
        if (this.columns != NOT_SET)
        {
            throw new IllegalArgumentException("Cannot modify coulumns when set.");
        }
        this.columns = defaultIfNull(columns, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    public boolean isAsteriskColumns()
    {
        return asteriskColumns;
    }

    void setAsteriskColumns()
    {
        this.asteriskColumns = true;
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

    /** Construct a table alias from provided table name */
    public static TableAlias of(TableAlias parent, QualifiedName table, String alias)
    {
        return new TableAlias(parent, table, alias, NOT_SET, false, null);
    }
    
    /** Construct a table alias from provided table name */
    public static TableAlias of(TableAlias parent, QualifiedName table, String alias, boolean asteriskColumns)
    {
        return new TableAlias(parent, table, alias, NOT_SET, asteriskColumns, null);
    }

    /** Construct a table alias from provided table name */
    public static TableAlias of(TableAlias parent, QualifiedName table, String alias, String[] columns)
    {
        return new TableAlias(parent, table, alias, columns, false, null);
    }

    /** Construct a table alias from provided table name */
    public static TableAlias of(TableAlias parent, QualifiedName table, String alias, Token token)
    {
        return new TableAlias(parent, table, alias, NOT_SET, false, token);
    }
}
