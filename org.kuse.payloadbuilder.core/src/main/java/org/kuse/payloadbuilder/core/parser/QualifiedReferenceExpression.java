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
import org.kuse.payloadbuilder.core.operator.Tuple.ComputedTuple;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/**
 * Expression of a qualified name type. Column reference with a nested path. Ie. field.subField.value
 **/
public class QualifiedReferenceExpression extends Expression implements HasIdentifier
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
    private final boolean initiallyResolved;

    /**
     * Mutable property. Temporary until query parser is rewritten to and a 2 pass analyze phase is done that resolves this.
     */
    private List<ResolvePath> resolvePaths;

    public QualifiedReferenceExpression(QualifiedName qname, int lambdaId, Token token)
    {
        this.qname = requireNonNull(qname, "qname");
        this.lambdaId = lambdaId;
        this.token = token;
        this.initiallyResolved = false;
    }

    public QualifiedReferenceExpression(QualifiedName qname, int lambdaId, List<ResolvePath> resolvePaths, Token token)
    {
        this.qname = requireNonNull(qname, "qname");
        this.lambdaId = lambdaId;
        this.token = token;
        this.initiallyResolved = true;
        setResolvePaths(resolvePaths);
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

    public boolean isInitiallyResolved()
    {
        return initiallyResolved;
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
    public String identifier()
    {
        return qname.getLast();
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
            Tuple tuple = context.getTuple();
            if (lambdaId >= 0)
            {
                value = context.getLambdaValue(lambdaId);

                //CSOFF
                // We have a target ordinal
                // If the lambda value was a Tuple, resolve the ordinal and first part value
                // else just keep on with what ever value was located in lambda
                if (path.targetTupleOrdinal >= 0 && value instanceof Tuple)
                {
                    tuple = (Tuple) value;
                    tuple = tuple.getTuple(path.targetTupleOrdinal);

                    // Nothing more to resolve here return tuple
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
            else if (tuple != null)
            {
                //CSOFF
                if (path.computedColumnIndex >= 0)
                {
                    if (!(tuple instanceof Tuple.ComputedTuple))
                    //CSON
                    {
                        throw new IllegalArgumentException("Expected a computed tuple but got " + tuple.getClass().getSimpleName());
                    }

                    return ((Tuple.ComputedTuple) tuple).getComputedValue(path.computedColumnIndex);
                }

                // Resolve target tuple
                tuple = tuple.getTuple(path.targetTupleOrdinal);

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
        final int sourceTupleOrinal;

        /**
         * The target tuple ordinal that this path refers to.
         */
        final int targetTupleOrdinal;

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
        final List<String> unresolvedPath;

        /**
         * This is a resolve path to a computed column and is pointing to the index of the computed value.
         *
         * <pre>
         * Assumes that the tuple in the stream is of
         * {@link ComputedTuple} type.
         * </pre>
         */
        final int computedColumnIndex;

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath)
        {
            this(sourceTupleOrdinal, targetTupleOrdinal, unresolvedPath, -1);
        }

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int computedColumnIndex)
        {
            this.sourceTupleOrinal = sourceTupleOrdinal;
            this.targetTupleOrdinal = targetTupleOrdinal;
            this.unresolvedPath = unmodifiableList(requireNonNull(unresolvedPath, "unresolvedPath"));
            this.computedColumnIndex = computedColumnIndex;
        }

        public int getTargetTupleOrdinal()
        {
            return targetTupleOrdinal;
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
