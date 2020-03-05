package com.viskan.payloadbuilder.codegen;

import com.viskan.payloadbuilder.TableAlias;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** Context used during code generation */
public class CodeGeneratorContext
{
    private final Map<String, AtomicInteger> varCountByPreix = new HashMap<>();
    // Generating a bi predicate
    // Row associations are outer and inner else row
    // TableAlias should reference inner relation
//    public boolean biPredicate;
    public TableAlias tableAlias;
//    boolean pretty;
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