package org.kuse.payloadbuilder.core.codegen;

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
    // Current lambda parameters in scope
    private final Set<String> lambdaParameters = new HashSet<>();
    private final Set<String> imports = new HashSet<>();

    /** Add import to code */
    public void addImport(String imp)
    {
        imports.add(imp);
    }

    public Set<String> getImports()
    {
        return imports;
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

    /**
     * Create a new code from context
     **/
    public ExpressionCode getCode()
    {
        return new ExpressionCode(newVar("v"));
    }
}
