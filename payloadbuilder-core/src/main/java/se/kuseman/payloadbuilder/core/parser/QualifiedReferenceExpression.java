package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.operator.StatementContext;

/**
 * Expression of a qualified name type. Column reference with a nested path. Ie. field.subField.value
 **/
public class QualifiedReferenceExpression extends Expression implements HasIdentifier
{
    private static final Map<DataType, String> TUPLE_ACCESSOR_METHOD = MapUtils.ofEntries(MapUtils.entry(DataType.INT, "getInt"), MapUtils.entry(DataType.LONG, "getLong"),
            MapUtils.entry(DataType.FLOAT, "getFloat"), MapUtils.entry(DataType.DOUBLE, "getDouble"), MapUtils.entry(DataType.BOOLEAN, "getBool"));

    private final QualifiedName qname;
    /**
     * <pre>
     * If this references a lambda parameter, this points to it's unique id in current scope.
     * Used to retrieve the current lambda value from evaluation context
     * </pre>
     */
    private final int lambdaId;
    private final Token token;
    private final ResolvePath[] resolvePaths;

    public QualifiedReferenceExpression(QualifiedName qname, int lambdaId, ResolvePath[] resolvePaths, Token token)
    {
        this.qname = requireNonNull(qname, "qname");
        this.lambdaId = lambdaId;
        this.resolvePaths = requireNonNull(resolvePaths, "resolvePaths");
        this.token = token;
    }

    @Override
    public QualifiedName getQualifiedName()
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

    public ResolvePath[] getResolvePaths()
    {
        return resolvePaths;
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
    public Object eval(IExecutionContext context)
    {
        int partIndex = 0;
        String[] parts = ArrayUtils.EMPTY_STRING_ARRAY;
        StatementContext sctx = (StatementContext) context.getStatementContext();
        Tuple tuple = sctx.getTuple();
        Object value = tuple;
        ResolvePath path = null;

        // Expression mode / test mode
        // We have no resolve path, simply use the full qualified name as column
        if (resolvePaths.length == 0)
        {
            parts = qname.getParts()
                    .toArray(ArrayUtils.EMPTY_STRING_ARRAY);
            if (lambdaId >= 0)
            {
                value = sctx.getLambdaValue(lambdaId);
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
            // TODO: sorceTupleOrdinal
            path = resolvePaths[0];
            parts = path.unresolvedPath;
            if (lambdaId >= 0)
            {
                value = sctx.getLambdaValue(lambdaId);
            }
        }

        return resolveValue(value, path, parts, partIndex);
    }

    /** Resolves value for provided tuple and path */
    // CSOFF
    private Object resolveValue(Object value, ResolvePath path, String[] unresolvedParts, int unresolvedPartIndex)
    // CSON
    {
        int partIndex = unresolvedPartIndex;
        /*
         * 1. Resolve target tuple ordinal 2. Resolve subPaths Sub path can result in either a tuple or a value 3. Resolve unresolved parts
         *
         */

        Object result = value;

        if (path != null)
        {
            /* Resolve tuple. Both target ordinal and sub paths if any */
            result = resolveTuple(result, path);

            // Nothing more to resolve here return result
            if (result == null
                    || (path.columnOrdinal == -1
                            && path.unresolvedPath.length == 0))
            {
                return result;
            }

            if (result instanceof Tuple)
            {
                // Get value for first unresolved part or column ordinal
                int ordinal = path.columnOrdinal >= 0 ? path.columnOrdinal
                        : ((Tuple) result).getColumnOrdinal(path.unresolvedPath[partIndex++]);

                result = ordinal >= 0 ? ((Tuple) result).getValue(ordinal)
                        : null;

                // Keep resolving tuples along the unresolved path
                while (result instanceof Tuple
                        && partIndex < path.unresolvedPath.length)
                {
                    ordinal = ((Tuple) result).getColumnOrdinal(path.unresolvedPath[partIndex++]);
                    result = ((Tuple) result).getValue(ordinal);
                }
            }
        }

        // Nothing more to resolve
        if (result == null
                || partIndex >= unresolvedParts.length)
        {
            return result;
        }

        if (result instanceof Map)
        {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) result;
            return MapUtils.traverse(map, partIndex, unresolvedParts);
        }

        throw new IllegalArgumentException("Cannot dereference '" + unresolvedParts[partIndex] + "' on value " + result);
    }

    private Object resolveTuple(Object input, ResolvePath path)
    {
        if (input == null)
        {
            return input;
        }

        Object result = input;
        // Resolve target tuple ordinal if needed
        if (result instanceof Tuple
                && !(path.targetTupleOrdinal == -1
                        || path.targetTupleOrdinal == ((Tuple) result).getTupleOrdinal()))
        {
            result = ((Tuple) result).getTuple(path.targetTupleOrdinal);
        }

        int length = path.subPath.length;
        for (int i = 0; i < length; i++)
        {
            if (result == null)
            {
                break;
            }
            ResolvePath sb = path.subPath[i];

            if (!(result instanceof Tuple))
            {
                throw new QueryException("Expected a tuple when travering resolve path but got: " + result);
            }

            if (sb.targetTupleOrdinal >= 0)
            {
                result = ((Tuple) result).getTuple(sb.targetTupleOrdinal);
            }
            else if (sb.columnOrdinal >= 0)
            {
                result = ((Tuple) result).getValue(sb.columnOrdinal);
            }
        }

        return result;
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
                // No sub paths
                || resolvePaths[0].subPath.length > 0
                // Multi part path ie. map access etc. not supported yet
                || resolvePaths[0].unresolvedPath.length > 1
                // Multi part with column ordinal
                || (resolvePaths[0].columnOrdinal >= 0
                        && resolvePaths[0].unresolvedPath.length >= 1)
                // Tuple access not supported yet
                || (resolvePaths[0].unresolvedPath.length == 0
                        && resolvePaths[0].columnOrdinal == -1))
        {
            return false;
        }

        return true;
    }

    // CSOFF
    @Override
    // CSON
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getExpressionCode();

        // TODO: lambda
        // TODO: only supported for single source tuple ordinals for now
        // TODO: map access etc.

        int targetOrdinal = -1;
        String column = lowerCase(qname.toDotDelimited());
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
                /*
                 * tupleOrdinal == -1
                 *
                 * <primitive> v_X = <default>; boolean isNull_X = true; { int c = tuple.getColumnOrdinal(-1, "col"); isNull_X = c >= 0 ? tuple.isNull(c) : true; v_X = isNull_x ? v_X :
                 * tuple.get<Primitive>(-1, c); }
                 */
                code.setCode(String.format("//%s\n" // qname
                                           + "%s %s = %s;\n" // dataType, resVar, defaultDataType
                                           + "boolean %s = true;\n" // nullVar
                                           + "{\n"
                                           + "  int c = %s;\n" // columnOrdinal or tuple.getColumnOrdinal(-1, \"%s\"), column
                                           + "  %s = c >= 0 ? %s.isNull(c) : true;\n" // nullVar, context tupleVar
                                           + "  %s = %s ? %s : %s.%s(c);\n" // resVar, nullVar, resVar, context tupleVar, dataType (capitalize)
                                           + "}\n",
                        qname, context.getJavaTypeString(type), code.getResVar(), context.getJavaDefaultValue(type), code.getNullVar(), columnOrdinal >= 0 ? columnOrdinal
                                : String.format("%s.getColumnOrdinal(\"%s\")", context.getTupleFieldName(), column),
                        code.getNullVar(), context.getTupleFieldName(), code.getResVar(), code.getNullVar(), code.getResVar(), context.getTupleFieldName(), typeMethod));
            }
            else
            {
                /*
                 * tupleOrdinal >= 0
                 *
                 * Object t_v_X = tuple.getTuple(targetOrdinal); boolean isNull_X = true; <primitive> v_X = <default>; if (t_v_X instanceof Tuple) { int c = ((Tuple)
                 * t_v_X).getColumnOrdinal(targetOrdinal, "col"); isNull_X = c >= 0 ? ((Tuple) t_v_X).isNull(c) : true; v_x = isNull_x ? v_x : ((Tuple) t_v_X).get<Primitive>(targetOrdinal, c); }
                 */
                String tupleVar = "t_" + code.getResVar();
                code.setCode(String.format("//%s\n" + "Tuple %s = %s.getTuple(%d);\n" // tupleVar, context tupleVar, targetOrdinal
                                           + "boolean %s = true;\n" // nullVar
                                           + "%s %s = %s;\n" // dataType, resVar, defaultValue
                                           + "if (%s != null)\n" // tupleVar
                                           + "{\n"
                                           + "  int c = %s;\n" // columnOrdinal else ((Tuple) %s).getColumnOrdinal(\"%s\") => tupleVar, targetOrdinal, columnName
                                           + "  %s = c >= 0 ? %s.isNull(c) : true;\n" // nullvar, tupleVar
                                           + "  %s = %s ? %s : %s.%s(c);\n" // resVar, nullVar, resVar, tupleVar, dataType (capitalize)
                                           + "}\n",
                        qname, tupleVar, context.getTupleFieldName(), targetOrdinal, code.getNullVar(), context.getJavaTypeString(type), code.getResVar(), context.getJavaDefaultValue(type), tupleVar,
                        columnOrdinal >= 0 ? columnOrdinal
                                : String.format("%s.getColumnOrdinal(\"%s\")", tupleVar, column),
                        code.getNullVar(), tupleVar, code.getResVar(), code.getNullVar(), code.getResVar(), tupleVar, typeMethod));
            }
        }
        else if (targetOrdinal == -1)
        {
            /*
             * tupleOrdinal == -1
             *
             * Object v_X = null; boolean isNull_X = true; { int c = tuple.getColumnOrdinal(-1, "col"); v_X = c >= 0 ? tuple.getValue(-1, c) : null; isNull_X = v_X == null; }
             */
            code.setCode(String.format("//%s\n" // qname
                                       + "Object %s = null;\n" // resVar
                                       + "boolean %s = true;\n" // nullVar
                                       + "{\n"
                                       + "  int c = %s;\n" // columnOrdinal or tuple.getColumnOrdinal(-1, \"%s\"), column
                                       + "  %s = c >= 0 ? %s.getValue(c) : null;\n" // resVar, context tupleVar
                                       + "  %s = %s == null;\n" // nullVar, resVar
                                       + "}\n",
                    qname, code.getResVar(), code.getNullVar(), columnOrdinal >= 0 ? columnOrdinal
                            : String.format("%s.getColumnOrdinal(\"%s\")", context.getTupleFieldName(), column),
                    code.getResVar(), context.getTupleFieldName(), code.getNullVar(), code.getResVar()));
        }
        else
        {
            /*
             * tupleOrdinal >= 0
             *
             * Object t_v_X = tuple.getTuple(targetOrdinal); boolean isNull_X = true; Object v_X = null; if (t_v_X instanceof Tuple) { int c = ((Tuple) t_v_X).getColumnOrdinal(targetOrdinal, "col");
             * v_X = c >= 0 ? tuple.getValue(-1, c) : null; isNull_X = v_X == null; }
             */
            String tupleVar = "t_" + code.getResVar();
            code.setCode(String.format("//%s\n" + "Tuple %s = %s.getTuple(%d);\n" // tupleVar, context tupleVar, targetOrdinal
                                       + "boolean %s = true;\n" // nullVar
                                       + "Object %s = null;\n" // resVar
                                       + "if (%s != null)\n" // tupleVar
                                       + "{\n"
                                       + "  int c = %s;\n" // columnOrdinal else ((Tuple) %s).getColumnOrdinal(\"%s\") => tupleVar, columnName
                                       + "  %s = c >= 0 ? %s.getValue(c) : null;\n" // resVar, tupleVar
                                       + "  %s = %s == null;\n" // nullvar, resVar
                                       + "}\n",
                    qname, tupleVar, context.getTupleFieldName(), targetOrdinal, code.getNullVar(), code.getResVar(), tupleVar, columnOrdinal >= 0 ? columnOrdinal
                            : String.format("%s.getColumnOrdinal(\"%s\")", tupleVar, column),
                    code.getResVar(), tupleVar, code.getNullVar(), code.getResVar()));
        }
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
            return lambdaId == that.lambdaId
                    && Arrays.equals(resolvePaths, that.resolvePaths);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return qname.toDotDelimited();
    }

    /**
     * Resolve information for a qualified reference - An optional pointer to an tuple ordinal - Unresolved path
     **/
    public static class ResolvePath
    {
        public static final ResolvePath[] EMPTY_ARRAY = new ResolvePath[0];

        /**
         * The source tuple ordinal that this path refers to. In case this qualifier is part of a multi tuple ordinal expression this value specifies if the path to use when the source tuple ordinal
         * matches
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
         * The target tuple ordinal the qualifier refers to.
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

        /**
         * Pre-defined column ordinal. A computed expression or temp table or a defined table. Then a lookup of column name to ordinal is not needed.
         */
        final int columnOrdinal;

        /** Column type if any */
        final DataType columnType;

        /**
         * A sub resolve path. This one is set if the current path points to a tuple-type value and destination is another path inside that tuple.
         */
        final ResolvePath[] subPath;

        public ResolvePath(ResolvePath source, List<String> unresolvedPath)
        {
            this(source.sourceTupleOrdinal, source.targetTupleOrdinal, unresolvedPath, source.columnOrdinal, null, source.subPath);
        }

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int columnOrdinal)
        {
            this(sourceTupleOrdinal, targetTupleOrdinal, unresolvedPath, columnOrdinal, null, null);
        }

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int columnOrdinal, ResolvePath[] subPath)
        {
            this(sourceTupleOrdinal, targetTupleOrdinal, unresolvedPath, columnOrdinal, null, subPath);
        }

        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int columnOrdinal, DataType dataType)
        {
            this(sourceTupleOrdinal, targetTupleOrdinal, unresolvedPath, columnOrdinal, dataType, null);
        }

        /** Construct a resolve path */
        public ResolvePath(int sourceTupleOrdinal, int targetTupleOrdinal, List<String> unresolvedPath, int columnOrdinal, DataType columnType, ResolvePath[] subPath)
        {
            this.sourceTupleOrdinal = sourceTupleOrdinal;
            this.targetTupleOrdinal = targetTupleOrdinal;
            this.unresolvedPath = requireNonNull(unresolvedPath, "unresolvedPath").toArray(ArrayUtils.EMPTY_STRING_ARRAY);
            this.columnOrdinal = requireNonNull(columnOrdinal, "columnOrdinal");
            this.columnType = columnType != null ? columnType
                    : DataType.ANY;
            this.subPath = subPath != null ? subPath
                    : EMPTY_ARRAY;

            for (ResolvePath sp : this.subPath)
            {
                if (sp.unresolvedPath.length > 0)
                {
                    throw new IllegalArgumentException("Cannot unresolved parts on sub paths.");
                }
            }
        }

        public int getSourceTupleOrdinal()
        {
            return sourceTupleOrdinal;
        }

        public int getTargetTupleOrdinal()
        {
            return targetTupleOrdinal;
        }

        public int getColumnOrdinal()
        {
            return columnOrdinal;
        }

        public String[] getUnresolvedPath()
        {
            return unresolvedPath;
        }

        public DataType getColumnType()
        {
            return columnType;
        }

        public ResolvePath[] getSubPath()
        {
            return subPath;
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
                        && columnType == that.columnType
                        && Arrays.equals(unresolvedPath, that.unresolvedPath)
                        && Arrays.equals(subPath, that.subPath);
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
