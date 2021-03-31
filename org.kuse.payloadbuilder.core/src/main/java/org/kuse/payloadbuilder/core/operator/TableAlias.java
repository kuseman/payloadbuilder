package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
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
    /** Unique ordinal of this alias
     * <pre>
     * Each table source in a select query gets assigned
     * a unique ordinal for fast access
     *  </pre>
     **/
    private final int tupleOrdinal;
    private final Type type;

    private String[] columns;
    /** Is all columns wanted for this table alias. */
    private boolean asteriskColumns;

    TableAlias(
            int tupleOrdinal,
            TableAlias parent,
            QualifiedName table,
            String alias,
            String[] columns,
            boolean asteriskColumns,
            Type type,
            Token token)
    {
        this.tupleOrdinal = tupleOrdinal;
        this.parent = parent;
        this.table = requireNonNull(table, "table");
        this.alias = requireNonNull(alias, "alias");
        this.columns = requireNonNull(columns, "columns");
        this.type = requireNonNull(type, "type");
        this.asteriskColumns = asteriskColumns;

        if (parent != null)
        {
            // Verify no siblings has the same alias
            if (parent.getChildAlias(alias) != null)
            {
                throw new ParseException("Alias " + alias + " already exists in scope.", token);
            }

            parent.childAliases.add(this);
        }
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
        return unmodifiableList(childAliases);
    }

    /** Get child alias for provided alias */
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

    /** Return sibling alias with provided alias */
    public TableAlias getSiblingAlias(String alias)
    {
        TableAlias current = this.parent;
        if (current == null)
        {
            return null;
        }
        return current.getChildAlias(alias);
    }

    /** Return sibling aliases */
    List<TableAlias> getSiblingAliases()
    {
        TableAlias current = this.parent;
        if (current == null)
        {
            return emptyList();
        }
        return unmodifiableList(current.childAliases);
    }

    /**
     * <pre>
     * Returns columns defined to for this table
     * If not empty these columns should be used when constructing {@link Row}'s
     * NOTE! If this is not empty values MUST come in the same order as in the array.
     * This is because the core plans references to index:es for faster access
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
        this.columns = ObjectUtils.defaultIfNull(columns, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    public boolean isAsteriskColumns()
    {
        return asteriskColumns;
    }

    void setAsteriskColumns()
    {
        this.asteriskColumns = true;
        // Propagate to child aliases
        if (type == Type.SUBQUERY)
        {
            childAliases.forEach(a -> a.setAsteriskColumns());
        }
    }

    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    /** Print alias hierarchy */
    String printHierarchy(int indent)
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
        return table + " [" + alias + ", " + tupleOrdinal + "]";
    }

    /** Used for test verification only */
    boolean isEqual(TableAlias other)
    {
        return tupleOrdinal == other.tupleOrdinal
            && table.equals(other.table)
            && alias.equals(other.alias)
            && Objects.deepEquals(columns, other.columns)
            && type == other.type
            && childAliases.size() == other.childAliases.size()
            && IntStream.range(0, childAliases.size()).allMatch(i -> childAliases.get(i).isEqual(other.childAliases.get(i)));
    }

    /** Alias type */
    public enum Type
    {
        TEMPORARY_TABLE,
        TABLE,
        FUNCTION,
        SUBQUERY;
    }

    /** Builder for aliases */
    public static class TableAliasBuilder
    {
        private final int tupleOrdinal;
        private final Type type;
        private final QualifiedName qname;
        private final String alias;
        private TableAlias parent;
        private String[] columns = NOT_SET;
        private final Token token;
        private boolean asteriskColumns;
        private List<TableAliasBuilder> childAliases;

        TableAliasBuilder(int tupleOrdinal, Type type, QualifiedName qname, String alias, Token token)
        {
            this.tupleOrdinal = tupleOrdinal;
            this.type = type;
            this.qname = qname;
            this.alias = alias;
            this.token = token;
        }

        //CSOFF
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
        //CSON

        /** Build alias */
        public TableAlias build()
        {
            TableAlias result = new TableAlias(tupleOrdinal, parent, qname, alias, columns, asteriskColumns, type, token);
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
        public static TableAliasBuilder of(int tupleOrdinal, Type type, QualifiedName qname, String alias)
        {
            return of(tupleOrdinal, type, qname, alias, null);
        }

        /** Create a builder out of provided tyee, qname, alias and token */
        public static TableAliasBuilder of(int tupleOrdinal, Type type, QualifiedName qname, String alias, Token token)
        {
            return new TableAliasBuilder(tupleOrdinal, type, qname, alias, token);
        }
    }
}
