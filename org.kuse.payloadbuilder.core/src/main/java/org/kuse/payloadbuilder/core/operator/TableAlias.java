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
    private final Type type;

    private String[] columns;
    /** Is all columns wanted for this table alias. */
    private boolean asteriskColumns;

    TableAlias(
            TableAlias parent,
            QualifiedName table,
            String alias,
            String[] columns,
            boolean asteriskColumns,
            Type type,
            Token token)
    {
        this.parent = parent;
        this.table = requireNonNull(table, "table");
        this.alias = requireNonNull(alias, "alias");
        this.columns = requireNonNull(columns, "columns");
        this.type = requireNonNull(type, "type");
        this.asteriskColumns = asteriskColumns;

        int parentIndex = -1;
        if (parent != null)
        {
            // Verify no siblings has the same alias
            if (parent.getChildAlias(alias) != null)
            {
                throw new ParseException("Alias " + alias + " already exists in scope.", token);
            }

            parent.childAliases.add(this);
            parentIndex = parent.childAliases.size() - 1;
        }
        this.parentIndex = parentIndex;
    }

    public Type getType()
    {
        return type;
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

    /**
     * <pre>
     * Returns columns collected for this alias in the query.
     * If not empty these columns should be used when constructing {@link Row}'s
     * </pre>
     */
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
            && alias.equals(other.alias)
            && Objects.deepEquals(columns, other.columns)
            && type == other.type
            && childAliases.size() == other.childAliases.size()
            && IntStream.range(0, childAliases.size()).allMatch(i -> childAliases.get(i).isEqual(other.childAliases.get(i)));
    }

    public enum Type
    {
        TABLE,
        FUNCTION,
        SUBQUERY;
    }

    //    /** Construct a table alias from provided table name */
    //    public static TableAlias of(TableAlias parent, QualifiedName table, String alias)
    //    {
    //        return new TableAlias(parent, table, alias, NOT_SET, false, Type.TABLE, null);
    //    }
    //
    //    /** Construct a table alias from provided table name */
    //    public static TableAlias of(TableAlias parent, QualifiedName table, String alias, boolean asteriskColumns)
    //    {
    //        return new TableAlias(parent, table, alias, NOT_SET, asteriskColumns, Type.TABLE, null);
    //    }
    //
    //    /** Construct a table alias from provided table name */
    //    public static TableAlias of(TableAlias parent, QualifiedName table, String alias, String[] columns)
    //    {
    //        return new TableAlias(parent, table, alias, columns, false, Type.TABLE, null);
    //    }
    //
    //    /** Construct a table alias from provided parent, table and alias */
    //    public static TableAlias of(TableAlias parent, QualifiedName table, String alias, Token token)
    //    {
    //        return new TableAlias(parent, table, alias, NOT_SET, false, Type.TABLE, token);
    //    }
    //
    //    /** Construct */
    //    public static TableAlias of(TableAlias parent, QualifiedName table, String alias, String[] columns, Type type, Token token)
    //    {
    //        return new TableAlias(parent, table, alias, defaultIfNull(columns, NOT_SET), false, type, token);
    //    }
    //
    /** */
    public static class TableAliasBuilder
    {
        private final Type type;
        private final QualifiedName qname;
        private final String alias;
        private TableAlias parent;
        private String[] columns = NOT_SET;
        private final Token token;
        private boolean asteriskColumns;
        private List<TableAliasBuilder> childAliases;

        TableAliasBuilder(Type type, QualifiedName qname, String alias, Token token)
        {
            this.type = type;
            this.qname = qname;
            this.alias = alias;
            this.token = token;
        }

        public TableAliasBuilder parent(TableAlias parent)
        {
            this.parent = requireNonNull(parent);
            return this;
        }

        public TableAliasBuilder columns(String[] columns)
        {
            this.columns = requireNonNull(columns);
            return this;
        }

        public TableAliasBuilder asteriskColumns()
        {
            this.asteriskColumns = true;
            return this;
        }

        public TableAliasBuilder children(List<TableAliasBuilder> childAliases)
        {
            this.childAliases = requireNonNull(childAliases);
            return this;
        }

        public TableAlias build()
        {
            TableAlias result = new TableAlias(parent, qname, alias, columns, asteriskColumns, type, token);
            if (childAliases != null)
            {
                childAliases.forEach(c ->
                {
                    c.parent = result;
                    c.build();
                });
            }
            return result;
        }

        /** Create a builder out of provided tyee, qname and alias */
        public static TableAliasBuilder of(Type type, QualifiedName qname, String alias)
        {
            return of(type, qname, alias, null);
        }

        /** Create a builder out of provided tyee, qname, alias and token */
        public static TableAliasBuilder of(Type type, QualifiedName qname, String alias, Token token)
        {
            return new TableAliasBuilder(type, qname, alias, token);
        }
    }
}
