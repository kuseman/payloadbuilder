package org.kuse.payloadbuilder.core.codegen;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.kuse.payloadbuilder.core.operator.TableAlias;

/** Context used during code generation */
public class CodeGeneratorContext
{
    private final Map<String, AtomicInteger> varCountByPreix = new HashMap<>();
    private TableAlias tableAlias;
    private final long now = System.currentTimeMillis();
    private final String rowVarName = "row";
    // Current lambda parameters in scope
    private final Set<String> lambdaParameters = new HashSet<>();

    public long getNow()
    {
        return now;
    }

    public String getRowVarName()
    {
        return rowVarName;
    }

    public TableAlias getTableAlias()
    {
        return tableAlias;
    }

    public void setTableAlias(TableAlias tableAlias)
    {
        this.tableAlias = tableAlias;
    }

    /** Allocate a new unique variable name */
    public String newVar(String prefix)
    {
        AtomicInteger count = varCountByPreix.computeIfAbsent(prefix, key -> new AtomicInteger());
        return prefix + "_" + count.getAndIncrement();
    }

    /** Adds provided identifiers to lambda scope */
    public void addLambdaParameters(List<String> identifiers)
    {
        lambdaParameters.addAll(identifiers);
    }

    /** Removes provided identifiers from lambda scope */
    public void removeLambdaParameters(List<String> identifiers)
    {
        lambdaParameters.removeAll(identifiers);
    }

    /** Checks if provided identifier is a lambda parameter */
    public boolean containsLambda(String identifier)
    {
        return lambdaParameters.contains(identifier);
    }
}
