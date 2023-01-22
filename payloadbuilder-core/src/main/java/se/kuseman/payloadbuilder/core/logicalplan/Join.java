package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.ObjectUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;

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

    // /**
    // * Field that is set after optimization of predicate push down. This will be later on used to ask catalogs to handle predicates to push down the filters even further.
    // */
    // private final List<AnalyzePair> predicatePairs;

    /** Flag that indicates that inner and outer has switched places so we need to take that into consideration when generating schema */
    private final boolean switchedInputs;

    /** Constructor used during column resolving */
    public Join(ILogicalPlan outer, ILogicalPlan inner, Type type, String populateAlias, IExpression condition, Set<Column> outerReferences, boolean switchedInputs)
    {
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.type = requireNonNull(type, "type");
        this.populateAlias = populateAlias;
        this.condition = condition;
        this.outerReferences = ObjectUtils.defaultIfNull(outerReferences, emptySet());
        // this.predicatePairs = null;
        this.switchedInputs = switchedInputs;
    }

    // /** Constructor used during predicate push down */
    // public Join(ILogicalPlan left, ILogicalPlan right, Type type, String populateAlias, List<AnalyzePair> predicatePairs, Set<Column> outerReferences, boolean switchedInputs)
    // {
    // this.outer = requireNonNull(left, "left");
    // this.inner = requireNonNull(right, "right");
    // this.type = requireNonNull(type, "type");
    // this.populateAlias = populateAlias;
    // this.predicatePairs = requireNonNull(predicatePairs, "predicatePairs");
    // this.condition = AnalyzeResult.getPredicate(predicatePairs);
    // this.outerReferences = ObjectUtils.defaultIfNull(outerReferences, emptySet());
    // this.switchedInputs = switchedInputs;
    // }

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

    // public List<AnalyzePair> getPredicatePairs()
    // {
    // return predicatePairs;
    // }

    public Set<Column> getOuterReferences()
    {
        return outerReferences;
    }

    public boolean isSwitchedInputs()
    {
        return switchedInputs;
    }

    @Override
    public Schema getSchema()
    {
        Schema outerSchema = switchedInputs ? inner.getSchema()
                : outer.getSchema();

        List<Column> columns = new ArrayList<>(outerSchema.getColumns());

        Schema innerSchema = switchedInputs ? outer.getSchema()
                : inner.getSchema();

        if (populateAlias != null)
        {
            // Copy table source from inner schema if any exists
            ColumnReference colRef = innerSchema.getColumns()
                    .get(0)
                    .getColumnReference();
            colRef = colRef != null ? colRef.rename(populateAlias)
                    : null;

            columns.add(new Column(populateAlias, ResolvedType.tupleVector(innerSchema), colRef));
        }
        else
        {
            columns.addAll(innerSchema.getColumns());
        }

        return new Schema(columns);
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
        if (obj instanceof Join)
        {
            Join that = (Join) obj;
            return outer.equals(that.outer)
                    && inner.equals(that.inner)
                    && type == that.type
                    && Objects.equals(condition, that.condition)
                    && outerReferences.equals(that.outerReferences)
                    && switchedInputs == that.switchedInputs
                    && Objects.equals(populateAlias, that.populateAlias);
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
               + (!outerReferences.isEmpty() ? ", outerReferences: " + outerReferences
                       : "")
               + (switchedInputs ? ", switched inputs"
                       : "")
               + (populateAlias != null ? ", populate (" + populateAlias + ")"
                       : "");
    }

    /** Type of join */
    public enum Type
    {
        INNER,
        LEFT,
        RIGHT
        // CROSS,
        // CROSS_APPLY,
        // OUTER_APPLY
    }
}
