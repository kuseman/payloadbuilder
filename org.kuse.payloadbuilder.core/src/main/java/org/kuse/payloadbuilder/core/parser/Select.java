package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.Objects;

import org.kuse.payloadbuilder.core.operator.TableAlias.Type;

/** Select */
public class Select extends ASelectNode
{
    private final List<SelectItem> selectItems;
    private final TableSourceJoined from;
    private final Table into;
    private final Expression topExpression;
    private final Expression where;
    private final List<Expression> groupBy;
    private final List<SortItem> orderBy;
    private final For forOutput;
    /** Computed expressions found during resolve phase for this select */
    private final List<Expression> computedExpressions;

    public Select(List<SelectItem> selectItems,
            TableSourceJoined from,
            Table into,
            Expression topExpression,
            Expression where,
            List<Expression> groupBy,
            List<SortItem> orderBy,
            For forOutput,
            List<Expression> computedExpressions)
    {
        this.selectItems = requireNonNull(selectItems, "selectItems");
        this.from = from;
        this.into = into;
        this.topExpression = topExpression;
        this.where = where;
        this.groupBy = requireNonNull(groupBy, "groupBy");
        this.orderBy = requireNonNull(orderBy, "orderBy");
        this.forOutput = forOutput;
        this.computedExpressions = requireNonNull(computedExpressions, "computedExpressions");

        if (into != null && into.getTableAlias() != null && into.getTableAlias().getType() != Type.TEMPORARY_TABLE)
        {
            throw new ParseException("Can only insert into temporary tables", into.getToken());
        }
    }

    public List<SelectItem> getSelectItems()
    {
        return selectItems;
    }

    public TableSourceJoined getFrom()
    {
        return from;
    }

    public Table getInto()
    {
        return into;
    }

    public Expression getTopExpression()
    {
        return topExpression;
    }

    public Expression getWhere()
    {
        return where;
    }

    public List<Expression> getGroupBy()
    {
        return groupBy;
    }

    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }

    public For getForOutput()
    {
        return forOutput;
    }

    public List<Expression> getComputedExpressions()
    {
        return computedExpressions;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    /** Type of FOR in select. Used when computing scalar values from sub queries */
    public enum For
    {
        /** Object output */
        OBJECT,
        /** Array output */
        ARRAY,
        /** Object array output */
        OBJECT_ARRAY;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(selectItems, from);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Select)
        {
            Select that = (Select) obj;
            return selectItems.equals(that.selectItems)
                && Objects.equals(from, that.from)
                && Objects.equals(into, that.into)
                && Objects.equals(topExpression, that.topExpression)
                && Objects.equals(where, that.where)
                && groupBy.equals(that.groupBy)
                && orderBy.equals(that.orderBy)
                && forOutput == that.forOutput
                && computedExpressions.equals(that.computedExpressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT");
        sb.append(System.lineSeparator());
        sb.append(selectItems.stream().map(s -> s.toString()).collect(joining("," + System.lineSeparator(), "", System.lineSeparator())));
        if (from != null)
        {
            sb.append("FROM ").append(from);
        }
        if (where != null)
        {
            sb.append(System.lineSeparator());
            sb.append("WHERE ").append(where);
        }

        if (!orderBy.isEmpty())
        {
            sb.append(System.lineSeparator());
            sb.append("ORDER BY ");
            sb.append(orderBy.stream().map(o -> o.toString()).collect(joining(", ")));
        }
        return sb.toString();
    }
}
