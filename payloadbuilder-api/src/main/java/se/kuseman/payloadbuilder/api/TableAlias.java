package se.kuseman.payloadbuilder.api;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.utils.StringUtils.equalsIgnoreCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.utils.StringUtils;

/**
 * Domain of a table alias
 *
 * <pre>
 *  Table alias is a representation of a all table sources
 *  and their info for a query.
 *
 *  ie.
 *
 *  select *
 *  from tableA a
 *  inner join
 *  (
 *    select *
 *    from tableC c
 *    where c.id &gt; 10
 *  ) b
 *    on b.id = a.id
 *
 *  Alias hierarchy:
 *
 *  ROOT
 *    tableA (a)   (ordinal = 0)
 *    SubQuery (b) (ordinal = 1)
 *      ROOT        (Each sub query is a new query and hence a ROOT node)
 *        tableC   (ordinal 0 + start ordinal = 2)       (ordinal is reset for each query tree
 *                                                        but with an initial counter to keep uniqueness through out whole query)
 *
 *  ie.
 *
 *  select *
 *  from tableA a
 *  inner join
 *  (
 *    select *
 *    from tableC c
 *    where c.id &gt; 10
 *
 *    union all         (Not implemented yet, but used for demonstrating alias hierarchy)
 *
 *    select *
 *    from tableD c
 *    where c.id &gt; 10
 *
 *  ) b
 *    on b.id = a.id
 *  inner join tableE e
 *    on e.id = a.id
 *
 *  Alias hierarchy:
 *
 *  ROOT
 *    tableA (a)   (ordinal = 0)
 *    SubQuery (b) (ordinal = 1)
 *      ROOT
 *        tableC   (ordinal 0 + start ordinal = 2)
 *      ROOT
 *        tableB   (ordinal 0 + start ordinal = 2)
 *    tableE  (e)  (ordinal = 3)                    (max ordinal of prev subquery was 2)
 *
 *  CompositeTuple
 *    Row (a)                   (ordinal = 0)
 *    Row (tableC or tableD)    (ordinal = 2)
 *    Row (tableE)              (ordinal = 3)
 * </pre>
 **/
public class TableAlias
{
    private final TableAlias parent;
    private final QualifiedName table;
    private final String alias;
    private final List<TableAlias> childAliases = new ArrayList<>();
    private final TableMeta tableMeta;

    /**
     * Unique ordinal of this alias
     *
     * <pre>
     * Each table source in a select query gets assigned
     * a unique ordinal for fast access
     * </pre>
     **/
    private final int tupleOrdinal;
    private final Type type;

    TableAlias(int tupleOrdinal, TableAlias parent, boolean connectAsChild, QualifiedName table, String alias, TableMeta tableMeta, Type type)
    {
        this.tupleOrdinal = tupleOrdinal;
        this.parent = parent;
        this.table = requireNonNull(table, "table");
        this.alias = requireNonNull(alias, "alias");
        this.tableMeta = tableMeta;
        this.type = requireNonNull(type, "type");

        if (parent != null)
        {
            // Verify no siblings has the same alias
            if (parent.getChildAlias(alias) != null)
            {
                throw new RuntimeException("Alias " + alias + " already exists in scope.");
            }

            if (connectAsChild)
            {
                parent.childAliases.add(this);
            }
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
        List<TableAlias> children = childAliases;
        // This is a sub query then first child
        // is ROOT, search in it's children
        if (type == Type.SUBQUERY)
        {
            if (children.size() == 0)
            {
                return null;
            }
            children = children.get(0).childAliases;
        }

        int size = children.size();
        for (int i = 0; i < size; i++)
        {
            TableAlias child = children.get(i);
            if (equalsIgnoreCase(child.alias, alias))
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
    public List<TableAlias> getSiblingAliases()
    {
        TableAlias current = this.parent;
        if (current == null)
        {
            return emptyList();
        }
        return unmodifiableList(current.childAliases);
    }

    /**
     * Returns the number of tuples on this alias level
     *
     * <pre>
     * Consider a select
     *
     * select *
     * from tableA a
     * inner join
     * (
     *   select *
     *   from tableC c
     *   inner join tableD d
     *     on d.col = c.col
     * ) b
     *   b.col = a.col
     *
     * The tuple stream will be
     *
     * CompositeTuple
     *   Row (a)
     *   CompositeTuple
     *     Row (c)
     *     Row (d)
     *
     * That will be flattened to:
     *
     * CompositeTuple
     *   Row (a)
     *   Row (c)
     *   Row (d)
     *
     * Count for alias a, b will be 3
     * Count for alias c, d will be 2
     * </pre>
     */
    public int getTupleCountCountOnLevel()
    {
        requireNonNull(parent);

        int count = 0;
        int size = parent.childAliases.size();
        for (int i = 0; i < size; i++)
        {
            TableAlias alias = parent.childAliases.get(i);
            if (alias.type == Type.SUBQUERY)
            {
                TableAlias subQueryAlias = alias.childAliases.get(0);
                // TODO: when composite sub queries like union etc. comes into play this might need a rewrite
                count += subQueryAlias.getChildAliases()
                        .size() > 0 ? subQueryAlias.childAliases.get(0)
                                .getTupleCountCountOnLevel()
                                : 1;
            }
            else
            {
                count++;
            }
        }
        return count;
    }

    /** Return table meta for this alias if any */
    public TableMeta getTableMeta()
    {
        return tableMeta;
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
        if (tableMeta != null)
        {
            sb.append(" ")
                    .append(tableMeta);
        }
        sb.append(System.lineSeparator());

        if (childAliases != null)
        {
            childAliases.forEach(a -> sb.append(a.printHierarchy(indent + 1)));
        }

        return sb.toString();
    }

    // NOTE! No hashCode or equals since
    // this structure is used during build and changes while located in
    // a Map and hence cannot have other than identity hashCode//equals

    @Override
    public String toString()
    {
        return table + " [" + alias + ", " + tupleOrdinal + "]";
    }

    /** Used for test verification only */
    public boolean isEqual(TableAlias other)
    {
        return tupleOrdinal == other.tupleOrdinal
                && table.equals(other.table)
                && alias.equals(other.alias)
                && Objects.equals(tableMeta, other.tableMeta)
                && type == other.type
                && childAliases.size() == other.childAliases.size()
                && IntStream.range(0, childAliases.size())
                        .allMatch(i -> childAliases.get(i)
                                .isEqual(other.childAliases.get(i)));
    }

    /** Alias type */
    public enum Type
    {
        ROOT,
        TEMPORARY_TABLE,
        TABLE,
        FUNCTION,
        SUBQUERY;
    }

    /** Create a builder out of provided tyee, qname and alias */
    public static TableAliasBuilder of(int tupleOrdinal, Type type, QualifiedName qname, String alias)
    {
        return TableAliasBuilder.of(tupleOrdinal, type, qname, alias);
    }

    /** Builder for aliases */
    public static class TableAliasBuilder
    {
        private final int tupleOrdinal;
        private final Type type;
        private final QualifiedName qname;
        private final String alias;
        private TableAlias parent;
        private TableMeta tableMeta;
        private List<TableAliasBuilder> childAliases;
        private boolean connectAsChild = true;

        TableAliasBuilder(int tupleOrdinal, Type type, QualifiedName qname, String alias)
        {
            this.tupleOrdinal = tupleOrdinal;
            this.type = type;
            this.qname = qname;
            this.alias = alias;
        }

        // CSOFF
        public TableAliasBuilder parent(TableAlias parent)
        {
            if (parent == null)
            {
                return this;
            }

            parent(parent, true);
            return this;
        }

        public TableAliasBuilder parent(TableAlias parent, boolean connectAsChild)
        {
            this.parent = requireNonNull(parent);
            this.connectAsChild = connectAsChild;
            return this;
        }

        public TableAliasBuilder tableMeta(TableMeta tableMeta)
        {
            this.tableMeta = tableMeta;
            return this;
        }

        public TableAliasBuilder children(List<TableAliasBuilder> childAliases)
        {
            this.childAliases = requireNonNull(childAliases);
            return this;
        }
        // CSON

        /** Build alias */
        public TableAlias build()
        {
            TableAlias result = new TableAlias(tupleOrdinal, parent, connectAsChild, qname, alias, tableMeta, type);
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

        /** Create a builder out of provided tyee, qname, alias and token */
        public static TableAliasBuilder of(int tupleOrdinal, Type type, QualifiedName qname, String alias)
        {
            return new TableAliasBuilder(tupleOrdinal, type, qname, alias);
        }
    }
}