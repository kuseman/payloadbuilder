package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.unmodifiableList;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;
import se.kuseman.payloadbuilder.core.physicalplan.PlanUtils;

/** Sink for inserting into temporary tables. */
public class SelectIntoTempTableSink implements IDatasink
{
    private static final QualifiedName INDICES = QualifiedName.of("indices");
    private static final QualifiedName CACHEPREFIX = QualifiedName.of("cacheprefix");
    private static final QualifiedName CACHETTL = QualifiedName.of("cachettl");
    private final QualifiedName table;
    private final List<Index> indices;
    private final IExpression cachePrefixExpression;
    private final IExpression cacheTtlExpression;

    SelectIntoTempTableSink(QualifiedName table, List<Option> options, boolean selectInto)
    {
        this.table = table;
        OptionsData optionsData = getOptionsData(table, options);
        this.indices = optionsData.indices;
        this.cachePrefixExpression = optionsData.cachePrefixExpression;
        this.cacheTtlExpression = optionsData.cacheTtlExpression;

        if (cachePrefixExpression != null
                && cacheTtlExpression == null)
        {
            throw new IllegalArgumentException("CacheTTL hint is mandatory to enable cache on temporary table: #" + table.extract(1));
        }

        if (!selectInto)
        {
            throw new IllegalArgumentException("Insert into is not supported");
        }
    }

    @Override
    public void execute(IExecutionContext context, TupleIterator input)
    {
        // Strip the # prefix, we don't want that when looking up tables
        QualifiedName table = this.table.extract(1)
                .toLowerCase();

        // Find out if result should be cached
        QualifiedName cacheName = null;
        Duration cacheTtl = null;

        if (cacheTtlExpression != null)
        {
            ValueVector vector;
            if (cachePrefixExpression != null)
            {
                vector = cachePrefixExpression.eval(TupleVector.CONSTANT, context);
                if (vector.isNull(0))
                {
                    throw new QueryException("Cache prefix expression: " + cachePrefixExpression + " evaluated to null");
                }
                cacheName = table.prepend(vector.valueAsString(0));
            }
            else
            {
                cacheName = table;
            }

            vector = cacheTtlExpression.eval(TupleVector.CONSTANT, context);

            try
            {
                cacheTtl = vector.isNull(0) ? null
                        : Duration.parse(String.valueOf(vector.valueAsObject(0)));
            }
            catch (DateTimeParseException e)
            {
                throw new IllegalArgumentException(String.valueOf(vector.valueAsObject(0)) + " cannot be parsed as a Duration. See java.time.Duration#parse");
            }
        }

        Supplier<TemporaryTable> tempTableSupplier = () -> new TemporaryTable(PlanUtils.concat(context, input), indices);
        TemporaryTable temporaryTable;
        if (cacheTtl != null)
        {
            temporaryTable = ((QuerySession) context.getSession()).getTempTableCache()
                    .computIfAbsent(cacheName, cacheTtl, tempTableSupplier);
        }
        else
        {
            temporaryTable = tempTableSupplier.get();
        }

        ((QuerySession) context.getSession()).setTemporaryTable(table, temporaryTable);
    }

    public List<Index> getIndices()
    {
        return indices;
    }

    private record OptionsData(List<Index> indices, IExpression cachePrefixExpression, IExpression cacheTtlExpression)
    {
    }

    private static OptionsData getOptionsData(QualifiedName table, List<Option> options)
    {
        IExpression cachePrefixExpression = null;
        IExpression cacheTtlExpression = null;
        List<Index> indices = new ArrayList<>();
        for (Option option : options)
        {
            QualifiedName qname = option.getOption();
            IExpression expression = option.getValueExpression();

            if (qname.equalsIgnoreCase(INDICES))
            {
                if (!expression.isConstant())
                {
                    throw new CompileException("Indices option must be constant. Table: " + table.extract(1));
                }
                else if (!expression.getType()
                        .equals(ResolvedType.array(ResolvedType.array(Column.Type.String))))
                {
                    throw new CompileException("Indices option must be of type Array<Array<String>>. Table: " + table.extract(1));
                }

                ValueVector vector = expression.eval(null);
                ValueVector array = vector.getArray(0);
                int size = array.size();
                for (int i = 0; i < size; i++)
                {
                    if (array.isNull(i))
                    {
                        continue;
                    }
                    ValueVector columnsVector = array.getArray(i);
                    int columnsSize = columnsVector.size();
                    if (columnsSize <= 0)
                    {
                        continue;
                    }
                    List<String> columns = new ArrayList<>(columnsSize);
                    for (int j = 0; j < columnsSize; j++)
                    {
                        if (columnsVector.isNull(j))
                        {
                            continue;
                        }
                        columns.add(columnsVector.getString(j)
                                .toString());
                    }
                    indices.add(new Index(table, columns, ColumnsType.ALL));
                }
            }
            else if (qname.equalsIgnoreCase(CACHEPREFIX))
            {
                cachePrefixExpression = option.getValueExpression();
            }
            else if (option.getOption()
                    .equalsIgnoreCase(CACHETTL))
            {
                cacheTtlExpression = option.getValueExpression();
            }
        }
        return new OptionsData(unmodifiableList(indices), cachePrefixExpression, cacheTtlExpression);
    }
}
