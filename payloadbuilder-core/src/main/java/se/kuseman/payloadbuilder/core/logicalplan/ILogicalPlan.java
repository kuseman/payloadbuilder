package se.kuseman.payloadbuilder.core.logicalplan;

import static java.lang.System.lineSeparator;
import static org.apache.commons.lang3.StringUtils.repeat;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;

/**
 * Definition of a logical plan.
 * 
 * <pre>
 * A logical plan is a plan composed of logical parts like a join, aggregate etc.
 * It's WHAT should be done not HOW. How stuff is done is the {@link IPhysicalPlan}'s
 * job.
 * 
 * This plan is the first step after parsing a query. It will be optimized as much as it can
 * be. How far this can be optimized depends on the participating catalogs schemas.
 * A catalog can have a static, dynamic or runtime schema. If the catalog have a static schema
 * the logical plan will have access to column names and types and more optimizations can be made.
 * A dynamic schema is known during creation of the physical plan and a new optimization iteration is
 * made.
 * A runtime schema is not known either in logical plan or physical plan only from actual rows
 * coming back from the query at runtime. This is the variant where many optimizations cannot be made at all before
 * query and will most likely be slow.
 * 
 * The logical plan will be cached and can be reused cross queries/catalogs as long as the query is the same
 * and the participating schemas are the same.
 * 
 * A physical plan is then made from the logical plan. Now new optimizations are made when the catalog
 * can return information about schema (dynamic schema) (ie. now a tenant/index/database is known and 
 * an actual schema can be returned).
 * 
 * A physical plan can be cached and reused if all participating schemas are static.
 * </pre>
 */
public interface ILogicalPlan
{
    /** Return the resulting schema for this plan */
    Schema getSchema();

    /** Return the children of this plan */
    List<ILogicalPlan> getChildren();

    /** Accept visitor */
    <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context);

    /** Print plan indented */
    default String print(int indent)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(repeat("  ", indent))
                .append(toString())
                .append(lineSeparator());

        for (ILogicalPlan child : getChildren())
        {
            sb.append(child.print(indent + 1));
        }

        return sb.toString();
    }
}
