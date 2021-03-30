package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ObjectUtils;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/**
 * Expression of a qualified name type. Column reference with a nested path. Ie. field.subField.value
 **/
public class QualifiedReferenceExpression extends Expression
{
    private final QualifiedName qname;
    /**
     * <pre>
     * If this references a lambda parameter, this points to it's unique id in current scope.
     * Used to retrieve the current lambda value from evaluation context
     * </pre>
     */
    private final int lambdaId;
    private final Token token;

    /**
     * Mutable property. Temporary until query parser is rewritten to and a 2 pass analyze phase is done that resolves this.
     */
    private List<ResolvePath> resolvePaths;

    public QualifiedReferenceExpression(QualifiedName qname, int lambdaId, Token token)
    {
        this.qname = requireNonNull(qname, "qname");
        this.lambdaId = lambdaId;
        this.token = token;
    }

    public QualifiedReferenceExpression(QualifiedName qname, int lambdaId, List<ResolvePath> resolvePaths, Token token)
    {
        this.qname = requireNonNull(qname, "qname");
        this.resolvePaths = requireNonNull(resolvePaths, "resolvePaths");
        this.lambdaId = lambdaId;
        this.token = token;
    }

    public QualifiedName getQname()
    {
        return qname;
    }

    public int getLambdaId()
    {
        return lambdaId;
    }

    public Token getToken()
    {
        return token;
    }

    void setResolvePaths(List<ResolvePath> resolvePaths)
    {
        if (this.resolvePaths != null)
        {
            throw new IllegalArgumentException("Resolve paths is already set");
        }
        requireNonNull(resolvePaths, "resolvePaths");
        if (resolvePaths.isEmpty())
        {
            throw new IllegalArgumentException("Empty resolve path");
        }
        this.resolvePaths = unmodifiableList(resolvePaths);
    }

    public List<ResolvePath> getResolvePaths()
    {
        return ObjectUtils.defaultIfNull(resolvePaths, emptyList());
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        Object value = null;
        int partIndex = 0;
        List<String> parts;

        // Expression mode
        if (resolvePaths == null)
        {
            parts = qname.getParts();
            if (lambdaId >= 0)
            {
                value = context.getLambdaValue(lambdaId);
                partIndex++;
            }
            else if (context.getTuple() != null)
            {
                value = context.getTuple().getValue(parts.get(partIndex++));
            }
        }
        else
        {
            // TODO: sorceTupleOrdinal
            ResolvePath path = resolvePaths.get(0);
            parts = path.unresolvedPath;
            if (lambdaId >= 0)
            {
                value = context.getLambdaValue(lambdaId);

                //CSOFF
                // We have a target ordinal
                // then we expect a tuple value
                if (value != null && path.targetTupleOrdinal >= 0)
                {
                    if (!(value instanceof Tuple))
                    //CSON
                    {
                        throw new IllegalArgumentException("Expected a Tuple from lambda value but got: " + value);
                    }

                    Tuple tuple = (Tuple) value;
                    tuple = tuple.getTuple(path.targetTupleOrdinal);

                    //CSOFF
                    if (tuple == null
                        //CSON
                        || parts.size() == 0)
                    {
                        return tuple;
                    }

                    // Get value for first part
                    value = tuple.getValue(parts.get(partIndex++));
                }
            }
            else if (context.getTuple() != null)
            {
                // Resolve target tuple
                Tuple tuple = context.getTuple().getTuple(path.targetTupleOrdinal);

                // Nothing more to resolve here return tuple
                //CSOFF
                if (tuple == null
                    //CSON
                    || parts.size() == 0)
                {
                    return tuple;
                }

                // Resolve first part
                value = tuple.getValue(parts.get(partIndex++));
            }
        }

        if (value == null
            || partIndex >= parts.size())
        {
            return value;
        }

        if (value instanceof Map)
        {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) value;
            return MapUtils.traverse(map, partIndex, parts);
        }

        throw new IllegalArgumentException("Cannot dereference value " + value);
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return true;
    }

    @Override
    public int hashCode()
    {
        return qname.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof QualifiedReferenceExpression)
        {
            QualifiedReferenceExpression that = (QualifiedReferenceExpression) obj;
            return qname.equals(that.qname)
                && lambdaId == that.lambdaId;
//                && Objects.equals(resolvePaths, that.resolvePaths);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return qname.toString();
    }

    /**
     * Resolve information for a qualified reference - An optional pointer to an tuple ordinal - Unresolved path
     **/
    public static class ResolvePath
    {
        /**
         * The source tuple ordinal that this path refers to. In case this qualifier is part of a multi tuple ordinal expression this value specifies
         * if the path to use when the source tuple ordinal matches
         *
         * <pre>
         *  ie.  unionall(aliasA, aliasB).map(x -> x.aliasC.id)
         *
         *       Here the reference x.aliasC.id is pointing to both aliasA and aliasB
         *       so depending on which one we get runtime we will end up with different target
         *       tuple ordinals for x.aliasC
         * </pre>
         */
        int sourceTupleOrinal;

        /**
         * The target tuple ordinal that this path refers to.
         */
        int targetTupleOrdinal;

        /**
         * Any left over path that needs to be resolved.
         *
         * <pre>
         *   ie. aliasA.aliasB.col.key.subkey
         *
         *   We found a target tuple ordinal for aliasA.aliasB
         *   then we have col.key.subkey left to resolve runtime from the target tuple
         * </pre>
         */
        List<String> unresolvedPath;

        ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath)
        {
            this.sourceTupleOrinal = sourceTupleOrdinal;
            this.targetTupleOrdinal = targetTupleOrdinal;
            this.unresolvedPath = unmodifiableList(requireNonNull(unresolvedPath, "unresolvedPath"));
        }

        public int getTargetTupleOrdinal()
        {
            return targetTupleOrdinal;
        }

        /** Empty path */
        boolean isEmpty()
        {
            return targetTupleOrdinal == -1
                && unresolvedPath.isEmpty();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sourceTupleOrinal, targetTupleOrdinal);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof ResolvePath)
            {
                ResolvePath that = (ResolvePath) obj;
                return sourceTupleOrinal == that.sourceTupleOrinal
                    && targetTupleOrdinal == that.targetTupleOrdinal
                    && unresolvedPath.equals(that.unresolvedPath);
            }
            return false;
        }
    }
}
