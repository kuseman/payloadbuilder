package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.antlr.v4.runtime.Token;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Tuple;
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

    /** Set resolve path, will be removed when a 2 phase operator build is in place */
    @Deprecated
    public void setResolvePaths(List<ResolvePath> resolvePaths)
    {
        if (this.resolvePaths != null)
        {
            return;
        }
        requireNonNull(resolvePaths, "resolvePaths");
        if (resolvePaths.isEmpty())
        {
            throw new IllegalArgumentException("Empty resolve path");
        }
        this.resolvePaths = unmodifiableList(resolvePaths);
    }

    @Deprecated
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
        String[] parts;
        Tuple tuple = context.getTuple();

        // Expression mode
        if (resolvePaths == null)
        {
            parts = qname.getParts().toArray(ArrayUtils.EMPTY_STRING_ARRAY);
            if (lambdaId >= 0)
            {
                value = context.getLambdaValue(lambdaId);
                partIndex++;
            }
            else if (tuple != null)
            {
                int ordinal = tuple.getColmnOrdinal(lowerCase(parts[partIndex++]));
                value = tuple.getValue(ordinal);
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
                        || parts.length == 0)
                    {
                        return tuple;
                    }

                    // Get value for first part
                    int ordinal = path.columnOrdinal >= 0 ? path.columnOrdinal : tuple.getColmnOrdinal(parts[partIndex++]);
                    value = tuple.getValue(ordinal);
                }
            }
            else if (tuple != null)
            {
                // Resolve target tuple
                tuple = tuple.getTuple(path.targetTupleOrdinal);

                // Nothing more to resolve here return tuple
                //CSOFF
                if (tuple == null ||
                //CSON
                    (path.columnOrdinal == -1
                        && parts.length == 0))
                {
                    return tuple;
                }

                // Resolve first part
                int ordinal = path.columnOrdinal >= 0 ? path.columnOrdinal : tuple.getColmnOrdinal(parts[partIndex++]);
                value = tuple.getValue(ordinal);
            }
        }

        if (value == null
            || partIndex >= parts.length)
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
    public boolean isCodeGenSupported()
    {
        return resolvePaths == null
            || (resolvePaths.size() == 1
                && resolvePaths.get(0).targetTupleOrdinal >= 0
                && (resolvePaths.get(0).unresolvedPath.length == 1
                    || resolvePaths.get(0).columnOrdinal >= 0));
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getCode();

        // TODO: lambda
        // TODO: only supported for single source tuple ordinals for now
        // TODO: map access etc.

        int targetOrdinal = -1;
        String column = lowerCase(qname.toString());
        int columnOrdinal = -1;
        if (!CollectionUtils.isEmpty(resolvePaths))
        {
            targetOrdinal = resolvePaths.get(0).targetTupleOrdinal;
            columnOrdinal = resolvePaths.get(0).columnOrdinal;
            if (columnOrdinal == -1)
            {
                column = resolvePaths.get(0).unresolvedPath[0];
            }
        }

        StringBuilder sb = new StringBuilder();

        // Expression mode then we don't have a target
        if (targetOrdinal == -1)
        {
            sb.append(String.format("// %s\n"
                + "Tuple t_%s = tuple;\n",
                    qname,
                    code.getResVar()));
        }
        else
        {
            /*
             * Tuple t_v_2 =  tuple.getTuple(ordinal);
             */
            sb.append(String.format("// %s\n"
                + "Tuple t_%s = tuple.getTuple(%d);\n",
                    qname,
                    code.getResVar(), targetOrdinal));
        }

        if (columnOrdinal >= 0)
        {
            /*
             * Object v_2 = t_v_2.getValue(1);
             */
            sb.append(String.format("Object %s = t_%s.getValue(%d);\n",
                    code.getResVar(), code.getResVar(), columnOrdinal));
        }
        else
        {
            /*
             * Object v_2 = t_v_2.getValue(t_v_2.getOrdinal("col"));
             */
            sb.append(String.format("Object %s = t_%s.getValue(t_%s.getColmnOrdinal(\"%s\"));\n",
                    code.getResVar(), code.getResVar(), code.getResVar(), column));
        }

        code.setCode(sb.toString());
        return code;
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
        final String[] unresolvedPath;

        /** Pre defined column ordinal. Typically a computed expression or similar */
        final int columnOrdinal;

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath)
        {
            this(sourceTupleOrdinal, targetTupleOrdinal, unresolvedPath, -1);
        }

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int columnOrdinal)
        {
            this.sourceTupleOrinal = sourceTupleOrdinal;
            this.targetTupleOrdinal = targetTupleOrdinal;
            this.unresolvedPath = requireNonNull(unresolvedPath, "unresolvedPath").toArray(ArrayUtils.EMPTY_STRING_ARRAY);
            this.columnOrdinal = columnOrdinal;
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
                    && columnOrdinal == that.columnOrdinal
                    && Arrays.equals(unresolvedPath, that.unresolvedPath);
            }
            return false;
        }

        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
        }
    }
}
