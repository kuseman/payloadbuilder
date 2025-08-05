package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.ObjectUtils.getIfNull;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/** Logical definition of a join */
public class Join implements ILogicalPlan
{
    private final ILogicalPlan outer;
    private final ILogicalPlan inner;
    private final Type type;
    /** Populate alias if this is populate join else null */
    private final String populateAlias;
    private final IExpression condition;
    private final Set<Column> outerReferences;

    /** Flag that indicates that inner and outer has switched places so we need to take that into consideration when generating schema */
    private final boolean switchedInputs;

    /** The outer schema if this join is a correlated type */
    private final Schema outerSchema;

    /** Constructor used during column resolving */
    public Join(ILogicalPlan outer, ILogicalPlan inner, Type type, String populateAlias, IExpression condition, Set<Column> outerReferences, boolean switchedInputs, Schema outerSchema)
    {
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.type = requireNonNull(type, "type");
        this.populateAlias = populateAlias;
        this.condition = condition;
        this.outerReferences = getIfNull(outerReferences, emptySet());
        this.switchedInputs = switchedInputs;
        this.outerSchema = getIfNull(outerSchema, Schema.EMPTY);
    }

    public ILogicalPlan getOuter()
    {
        return outer;
    }

    public ILogicalPlan getInner()
    {
        return inner;
    }

    public Type getType()
    {
        return type;
    }

    public String getPopulateAlias()
    {
        return populateAlias;
    }

    public IExpression getCondition()
    {
        return condition;
    }

    public Set<Column> getOuterReferences()
    {
        return outerReferences;
    }

    public boolean isSwitchedInputs()
    {
        return switchedInputs;
    }

    public Schema getOuterSchema()
    {
        return outerSchema;
    }

    /** Returns true of this join can have outer references otherwise false */
    public boolean canHaveOuterReferences()
    {
        // We support outer references with conditions when the inner is an expression scan
        // that way one can write joins like this:
        // select *
        // from tableA a
        // inner join (a.tableColumn) c
        // on c.key = 123
        if (condition != null
                && inner instanceof ExpressionScan)
        {
            return true;
        }

        // Outer/cross apply's (lateral joins) can have outer references
        if (condition == null
                && type != Join.Type.CROSS)
        {
            return true;
        }

        return false;
    }

    @Override
    public Schema getSchema()
    {
        Schema outerSchema = switchedInputs ? inner.getSchema()
                : outer.getSchema();

        Schema innerSchema = switchedInputs ? outer.getSchema()
                : inner.getSchema();

        return SchemaUtils.joinSchema(outerSchema, innerSchema, populateAlias);
    }

    @Override
    public List<ILogicalPlan> getChildren()
    {
        return asList(outer, inner);
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outer, inner);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof Join that)
        {
            return outer.equals(that.outer)
                    && inner.equals(that.inner)
                    && type == that.type
                    && Objects.equals(condition, that.condition)
                    && outerReferences.equals(that.outerReferences)
                    && switchedInputs == that.switchedInputs
                    && Objects.equals(populateAlias, that.populateAlias)
                    && outerSchema.equals(that.outerSchema);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Join (" + type
               + ")"
               + (condition != null ? ", condition: " + condition.toVerboseString()
                       : "")
               + (!outerReferences.isEmpty() ? ", outerReferences: " + outerReferences.stream()
                       .map(o ->
                       {
                           String str = o.toString();
                           TableSourceReference tableSource = SchemaUtils.getTableSource(o);
                           if (tableSource != null)
                           {
                               str += " (" + tableSource + ")";
                           }
                           return str;
                       })
                       .collect(joining(", "))
                       : "")
               + (switchedInputs ? ", switched inputs"
                       : "")
               + (populateAlias != null ? ", populate (" + populateAlias + ")"
                       : "")
               + (outerSchema.getSize() > 0 ? ", outer schema: " + outerSchema.getColumns()
                       .stream()
                       .map(o ->
                       {
                           String str = o.toString();
                           TableSourceReference tableSource = SchemaUtils.getTableSource(o);
                           if (tableSource != null)
                           {
                               str += " (" + tableSource + ")";
                           }
                           return str;
                       })
                       .collect(joining(", "))
                       : "");
    }

    /** Type of join */
    public enum Type
    {
        CROSS,
        INNER,
        LEFT,
        RIGHT
    }
}
