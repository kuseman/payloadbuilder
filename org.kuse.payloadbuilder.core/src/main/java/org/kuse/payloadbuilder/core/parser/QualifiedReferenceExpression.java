package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
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
    private static final Map<DataType, String> TUPLE_ACCESSOR_METHOD = MapUtils.ofEntries(
            MapUtils.entry(DataType.INT, "getInt"),
            MapUtils.entry(DataType.LONG, "getLong"),
            MapUtils.entry(DataType.FLOAT, "getFloat"),
            MapUtils.entry(DataType.DOUBLE, "getDouble"),
            MapUtils.entry(DataType.BOOLEAN, "getBool"));

    private static final ResolvePath[] EMPTY = new ResolvePath[0];

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
    private ResolvePath[] resolvePaths;

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
        this.resolvePaths = resolvePaths.toArray(EMPTY);
    }

    @Deprecated
    public ResolvePath[] getResolvePaths()
    {
        return ObjectUtils.defaultIfNull(resolvePaths, EMPTY);
    }

    @Override
    public String identifier()
    {
        return qname.getLast();
    }

    @Override
    public DataType getDataType()
    {
        if (resolvePaths == null)
        {
            return DataType.ANY;
        }

        int size = resolvePaths.length;
        DataType prev = null;
        for (int i = 0; i < size; i++)
        {
            if (prev == null)
            {
                prev = resolvePaths[i].columnType;
            }
            else if (prev != resolvePaths[i].columnType)
            {
                return DataType.ANY;
            }
        }

        return prev;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        Object value = null;
        int partIndex = 0;
        String[] parts = ArrayUtils.EMPTY_STRING_ARRAY;
        Tuple tuple = context.getStatementContext().getTuple();

        // Expression mode / test mode
        // We have no resolve path, simply use the full qualified name as column
        if (resolvePaths == null)
        {
            parts = qname.getParts().toArray(ArrayUtils.EMPTY_STRING_ARRAY);
            if (lambdaId >= 0)
            {
                value = context.getStatementContext().getLambdaValue(lambdaId);
                partIndex++;
            }
            else if (tuple != null)
            {
                int ordinal = tuple.getColumnOrdinal(lowerCase(parts[partIndex++]));
                value = tuple.getValue(ordinal);
            }
        }
        else
        {
            if (lambdaId >= 0)
            {
                value = context.getStatementContext().getLambdaValue(lambdaId);

                // If the lambda value was a Tuple, set tuple field
                // else null it to not resolve further down
                //CSOFF
                if (value instanceof Tuple)
                //CSON
                {
                    tuple = (Tuple) value;
                }
                else
                {
                    tuple = null;
                }
            }

            // TODO: sorceTupleOrdinal
            ResolvePath path = resolvePaths[0];
            parts = path.unresolvedPath;
            if (tuple != null)
            {
                value = resolve(tuple, path);
                //CSOFF
                if (value instanceof Tuple)
                //CSON
                {
                    value = resolveValue((Tuple) value, path, partIndex);
                }
                partIndex++;
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
        if (resolvePaths == null
            || lambdaId >= 0
            || resolvePaths.length != 1
            || resolvePaths[0].subTupleOrdinals.length != 0
            // Multi part path ie. map access etc. not supported yet
            || resolvePaths[0].unresolvedPath.length > 1
            // Tuple access not supported yet
            || (resolvePaths[0].unresolvedPath.length == 0
                && resolvePaths[0].columnOrdinal == -1))
        {
            return false;
        }

        return true;
    }

    //CSOFF
    @Override
    //CSON
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getExpressionCode();

        // TODO: lambda
        // TODO: only supported for single source tuple ordinals for now
        // TODO: map access etc.

        int targetOrdinal = -1;
        String column = lowerCase(qname.toString());
        int columnOrdinal = -1;
        DataType type = DataType.ANY;
        if (resolvePaths != null)
        {
            targetOrdinal = resolvePaths[0].targetTupleOrdinal;
            columnOrdinal = resolvePaths[0].columnOrdinal;
            if (columnOrdinal == -1)
            {
                column = resolvePaths[0].unresolvedPath[0];
            }
            type = resolvePaths[0].columnType;
        }

        String typeMethod = TUPLE_ACCESSOR_METHOD.get(type);
        // Special get-methods on tuple (getInt, getFloat etc.)
        if (typeMethod != null)
        {
            if (targetOrdinal == -1)
            {
                /* tupleOrdinal == -1
                 *
                 * <primitive> v_X = <default>;
                 * boolean isNull_X = true;
                 * {
                 *   int c = tuple.getColumnOrdinal(-1, "col");
                 *   isNull_X = c >= 0 ? tuple.isNull(c) : true;
                 *   v_X = isNull_x ? v_X : tuple.get<Primitive>(-1, c);
                 * }
                 */
                code.setCode(String.format("//%s\n"                      // qname
                    + "%s %s = %s;\n"                                    // dataType, resVar, defaultDataType
                    + "boolean %s = true;\n"                             // nullVar
                    + "{\n"
                    + "  int c = %s;\n"                                  // columnOrdinal or tuple.getColumnOrdinal(-1, \"%s\"), column
                    + "  %s = c >= 0 ? %s.isNull(c) : true;\n"           // nullVar, context tupleVar
                    + "  %s = %s ? %s : %s.%s(c);\n"                     // resVar, nullVar, resVar, context tupleVar, dataType (capitalize)
                    + "}\n",
                        qname,
                        type.getJavaTypeString(), code.getResVar(), type.getJavaDefaultValue(),
                        code.getNullVar(),
                        columnOrdinal >= 0 ? columnOrdinal : String.format("%s.getColumnOrdinal(\"%s\")", context.getTupleFieldName(), column),
                        code.getNullVar(), context.getTupleFieldName(),
                        code.getResVar(), code.getNullVar(), code.getResVar(), context.getTupleFieldName(), typeMethod));
            }
            else
            {
                /*
                 * tupleOrdinal >= 0
                 *
                 * Object t_v_X = tuple.getTuple(targetOrdinal);
                 * boolean isNull_X = true;
                 * <primitive> v_X = <default>;
                 * if (t_v_X instanceof Tuple)
                 * {
                 *   int c = ((Tuple) t_v_X).getColumnOrdinal(targetOrdinal, "col");
                 *   isNull_X = c >= 0 ? ((Tuple) t_v_X).isNull(c) : true;
                 *   v_x = isNull_x ? v_x :  ((Tuple) t_v_X).get<Primitive>(targetOrdinal, c);
                 * }
                 */
                String tupleVar = "t_" + code.getResVar();
                code.setCode(String.format("//%s\n"
                    + "Tuple %s = %s.getTuple(%d);\n"                        // tupleVar, context tupleVar, targetOrdinal
                    + "boolean %s = true;\n"                                 // nullVar
                    + "%s %s = %s;\n"                                        // dataType, resVar, defaultValue
                    + "if (%s != null)\n"                                    // tupleVar
                    + "{\n"
                    + "  int c = %s;\n"                                      // columnOrdinal else ((Tuple) %s).getColumnOrdinal(\"%s\") => tupleVar, targetOrdinal, columnName
                    + "  %s = c >= 0 ? %s.isNull(c) : true;\n"               // nullvar,  tupleVar
                    + "  %s = %s ? %s : %s.%s(c);\n"                         // resVar, nullVar, resVar, tupleVar, dataType (capitalize)
                    + "}\n",
                        qname,
                        tupleVar, context.getTupleFieldName(), targetOrdinal,
                        code.getNullVar(),
                        type.getJavaTypeString(), code.getResVar(), type.getJavaDefaultValue(),
                        tupleVar,
                        columnOrdinal >= 0 ? columnOrdinal : String.format("%s.getColumnOrdinal(\"%s\")", tupleVar, column),
                        code.getNullVar(), tupleVar,
                        code.getResVar(), code.getNullVar(), code.getResVar(), tupleVar, typeMethod));
            }
        }
        else if (targetOrdinal == -1)
        {
            /* tupleOrdinal == -1
            *
            * Object v_X = null;
            * boolean isNull_X = true;
            * {
            *   int c = tuple.getColumnOrdinal(-1, "col");
            *   v_X = c >= 0 ? tuple.getValue(-1, c) : null;
            *   isNull_X = v_X == null;
            * }
            */
            code.setCode(String.format("//%s\n"                      // qname
                + "Object %s = null;\n"                              // resVar
                + "boolean %s = true;\n"                             // nullVar
                + "{\n"
                + "  int c = %s;\n"                                  // columnOrdinal or tuple.getColumnOrdinal(-1, \"%s\"), column
                + "  %s = c >= 0 ? %s.getValue(c) : null;\n"         // resVar, context tupleVar
                + "  %s = %s == null;\n"                             // nullVar, resVar
                + "}\n",
                    qname,
                    code.getResVar(),
                    code.getNullVar(),
                    columnOrdinal >= 0 ? columnOrdinal : String.format("%s.getColumnOrdinal(\"%s\")", context.getTupleFieldName(), column),
                    code.getResVar(), context.getTupleFieldName(),
                    code.getNullVar(), code.getResVar()));
        }
        else
        {
            /*
             * tupleOrdinal >= 0
             *
             * Object t_v_X = tuple.getTuple(targetOrdinal);
             * boolean isNull_X = true;
             * Object v_X = null;
             * if (t_v_X instanceof Tuple)
             * {
             *   int c = ((Tuple) t_v_X).getColumnOrdinal(targetOrdinal, "col");
             *   v_X = c >= 0 ? tuple.getValue(-1, c) : null;
             *   isNull_X = v_X == null;
             * }
             */
            String tupleVar = "t_" + code.getResVar();
            code.setCode(String.format("//%s\n"
                + "Tuple %s = %s.getTuple(%d);\n"                           // tupleVar, context tupleVar, targetOrdinal
                + "boolean %s = true;\n"                                    // nullVar
                + "Object %s = null;\n"                                     // resVar
                + "if (%s != null)\n"                                       // tupleVar
                + "{\n"
                + "  int c = %s;\n"                                         // columnOrdinal else ((Tuple) %s).getColumnOrdinal(\"%s\") => tupleVar, columnName
                + "  %s = c >= 0 ? %s.getValue(c) : null;\n"                // resVar, tupleVar
                + "  %s = %s == null;\n"                                    // nullvar,  resVar
                + "}\n",
                    qname,
                    tupleVar, context.getTupleFieldName(), targetOrdinal,
                    code.getNullVar(),
                    code.getResVar(),
                    tupleVar,
                    columnOrdinal >= 0 ? columnOrdinal : String.format("%s.getColumnOrdinal(\"%s\")", tupleVar, column),
                    code.getResVar(), tupleVar,
                    code.getNullVar(), code.getResVar()));
        }
        return code;
    }

    /** Traverses to the target tuple with provided path */
    private Object resolve(Tuple tuple, ResolvePath path)
    {
        Object result = tuple;
        if (path.targetTupleOrdinal >= 0)
        {
            result = tuple.getTuple(path.targetTupleOrdinal);
        }

        if (result == null)
        {
            return null;
        }

        if (path.subTupleOrdinals.length > 0)
        {
            int length = path.subTupleOrdinals.length;
            for (int i = 0; i < length; i++)
            {
                result = getTuple(result).getSubTuple(path.subTupleOrdinals[i]);
                if (result == null)
                {
                    return null;
                }
            }
        }
        return result;
    }

    private Tuple getTuple(Object obj)
    {
        if (obj != null && !(obj instanceof Tuple))
        {
            throw new IllegalArgumentException("Expected a Tuple but got " + obj);
        }
        return (Tuple) obj;
    }

    /** Resolves value from provided with with provided path */
    private Object resolveValue(Tuple tuple, ResolvePath path, int partIndex)
    {
        // Nothing more to resolve here return tuple
        if (tuple == null
            || (path.columnOrdinal == -1
                && path.unresolvedPath.length == 0))
        {
            return tuple;
        }

        // Get value for first part
        int ordinal = path.columnOrdinal >= 0
            ? path.columnOrdinal
            : tuple.getColumnOrdinal(path.unresolvedPath[partIndex]);
        return tuple.getValue(ordinal);
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
        final int sourceTupleOrdinal;

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

        /** When resolved into a temp tables sub alias, this ordinal points to it */
        final int[] subTupleOrdinals;

        /** Column type if any */
        final DataType columnType;

        public ResolvePath(ResolvePath source, List<String> unresolvedPath)
        {
            this(source.sourceTupleOrdinal, source.targetTupleOrdinal, unresolvedPath, source.columnOrdinal, source.subTupleOrdinals, null);
        }

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int columnOrdinal)
        {
            this(sourceTupleOrdinal, targetTupleOrdinal, unresolvedPath, columnOrdinal, ArrayUtils.EMPTY_INT_ARRAY, null);
        }

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int columnOrdinal, DataType columnType)
        {
            this(sourceTupleOrdinal, targetTupleOrdinal, unresolvedPath, columnOrdinal, ArrayUtils.EMPTY_INT_ARRAY, columnType);
        }

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int columnOrdinal, int[] subTupleOrdinals)
        {
            this(sourceTupleOrdinal, targetTupleOrdinal, unresolvedPath, columnOrdinal, subTupleOrdinals, null);
        }

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int columnOrdinal, int[] subTupleOrdinals, DataType columnType)
        {
            this.sourceTupleOrdinal = sourceTupleOrdinal;
            this.targetTupleOrdinal = targetTupleOrdinal;
            this.unresolvedPath = requireNonNull(unresolvedPath, "unresolvedPath").toArray(ArrayUtils.EMPTY_STRING_ARRAY);
            this.columnOrdinal = columnOrdinal;
            this.subTupleOrdinals = subTupleOrdinals;
            this.columnType = columnType != null ? columnType : DataType.ANY;
        }

        public int getSourceTupleOrdinal()
        {
            return sourceTupleOrdinal;
        }

        public int getTargetTupleOrdinal()
        {
            return targetTupleOrdinal;
        }

        public String[] getUnresolvedPath()
        {
            return unresolvedPath;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sourceTupleOrdinal, targetTupleOrdinal);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof ResolvePath)
            {
                ResolvePath that = (ResolvePath) obj;
                return sourceTupleOrdinal == that.sourceTupleOrdinal
                    && targetTupleOrdinal == that.targetTupleOrdinal
                    && columnOrdinal == that.columnOrdinal
                    && Arrays.equals(unresolvedPath, that.unresolvedPath)
                    && Arrays.equals(subTupleOrdinals, that.subTupleOrdinals);
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
