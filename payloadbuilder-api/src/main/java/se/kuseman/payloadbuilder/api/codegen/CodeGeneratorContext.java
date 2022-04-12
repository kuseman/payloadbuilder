package se.kuseman.payloadbuilder.api.codegen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;

/** Context used during code generation */
public class CodeGeneratorContext
{
    private final Map<String, AtomicInteger> varCountByPrefix = new HashMap<>();
    // Current lambda parameters in scope
    private final Set<String> lambdaParameters = new HashSet<>();
    private final Set<String> imports = new HashSet<>();
    private final List<Object> references = new ArrayList<>();

    /** Current tuple field name */
    private String tupleFieldName = "tuple";

    /** Add import to code */
    public void addImport(String imp)
    {
        imports.add(imp);
    }

    public Set<String> getImports()
    {
        return imports;
    }

    /** Add a reference to generation context */
    public int addReference(Object reference)
    {
        int index = references.size();
        this.references.add(reference);
        return index;
    }

    /** Get references */
    public List<Object> getReferences()
    {
        return references;
    }

    /** Get current tuple field name */
    public String getTupleFieldName()
    {
        return tupleFieldName;
    }

    /** Set current tuple field name */
    public void setTupleFieldName(String tupleFieldName)
    {
        this.tupleFieldName = tupleFieldName;
    }

    /** Allocate a new unique variable name */
    public String newVar(String prefix)
    {
        AtomicInteger count = varCountByPrefix.computeIfAbsent(prefix, key -> new AtomicInteger());
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
     * Create a new expression code from context
     **/
    public ExpressionCode getExpressionCode()
    {
        return new ExpressionCode(newVar("v"), newVar("n"));
    }

    /**
     * Create a new projection code from context
     **/
    public ProjectionCode getProjectionCode()
    {
        return new ProjectionCode();
    }

    /** Return the java type string for provided data type */
    public String getJavaTypeString(DataType type)
    {
        switch (type)
        {
            case ANY:
                return "Object";
            case BOOLEAN:
                return "boolean";
            case DOUBLE:
                return "double";
            case FLOAT:
                return "float";
            case INT:
                return "int";
            case LONG:
                return "long";
            default:
                throw new IllegalArgumentException("Unkown type " + this);
        }
    }

    /** Get default value string for provided type */
    public String getJavaDefaultValue(DataType type)
    {
        switch (type)
        {
            case ANY:
                return "null";
            case BOOLEAN:
                return "false";
            case DOUBLE:
                return "0.0";
            case FLOAT:
                return "0.0f";
            case INT:
                return "0";
            case LONG:
                return "0l";
            default:
                throw new IllegalArgumentException("Unkown type " + this);
        }
    }
}
