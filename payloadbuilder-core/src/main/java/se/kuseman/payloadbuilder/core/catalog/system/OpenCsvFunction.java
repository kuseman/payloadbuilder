package se.kuseman.payloadbuilder.core.catalog.system;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** TVF for opening a CSV argument and transform into a {@link TupleIterator} */
class OpenCsvFunction extends TableFunctionInfo
{
    private static final CsvMapper MAPPER = new CsvMapper();
    static final QualifiedName COLUMN_SEPARATOR = QualifiedName.of("columnSeparator");
    static final QualifiedName COLUMN_HEADERS = QualifiedName.of("columnHeaders");

    public OpenCsvFunction()
    {
        super("opencsv");
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData functionData)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        CsvNodeData nodeData = context.getStatementContext()
                .getNodeData(functionData.getNodeId());
        if (nodeData != null
                && nodeData.totalBytes > 0)
        {
            float bytesPerSecond = nodeData.totalBytes / ((float) nodeData.totalMillis / 1000);
            properties.put("Bytes Per Second", FileUtils.byteCountToDisplaySize(bytesPerSecond) + " / s");
        }
        return properties;
    }

    @Override
    public Schema getSchema(IExecutionContext context, String catalogAlias, List<IExpression> arguments, List<Option> options)
    {
        CsvSchema csvSchema = getCsvSchema(context, options);
        List<String> columnNames = csvSchema.getColumnNames();
        if (columnNames.isEmpty())
        {
            return Schema.EMPTY;
        }

        return new Schema(columnNames.stream()
                .map(c -> Column.of(c, ResolvedType.STRING))
                .toList());
    }

    private CsvSchema getCsvSchema(IExecutionContext context, List<Option> options)
    {
        ValueVector vv = context.getOption(COLUMN_SEPARATOR, options);
        char columnSeparator = vv == null
                || vv.isNull(0) ? CsvSchema.DEFAULT_COLUMN_SEPARATOR
                        : vv.getString(0)
                                .toString()
                                .charAt(0);

        vv = context.getOption(COLUMN_HEADERS, options);
        String columnHeaders = vv == null
                || vv.isNull(0) ? null
                        : vv.getString(0)
                                .toString();

        CsvSchema headerSchema;

        if (columnHeaders == null)
        {
            headerSchema = CsvSchema.emptySchema()
                    .withColumnSeparator(columnSeparator)
                    .withHeader();
        }
        else
        {
            String[] columns = StringUtils.split(columnHeaders, String.valueOf(columnSeparator));
            Builder builder = CsvSchema.builder()
                    .setColumnSeparator(columnSeparator);
            for (String column : columns)
            {
                builder.addColumn(column);
            }
            headerSchema = builder.build();
        }

        return headerSchema;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData functionData)
    {
        ValueVector value = arguments.get(0)
                .eval(context);

        if (value.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        CsvParser parser;
        Closeable closable;
        final CsvSchema headerSchema = getCsvSchema(context, functionData.getOptions());
        try
        {
            Pair<Closeable, CsvParser> pair = getParser(value);
            closable = pair.getKey();
            parser = pair.getValue();
            parser.setSchema(headerSchema);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error reading CSV", e);
        }

        final CsvNodeData nodeData = functionData.getNodeId() >= 0 ? context.getStatementContext()
                .getOrCreateNodeData(functionData.getNodeId(), () -> new CsvNodeData())
                : new CsvNodeData();
        final BoundedInputStream cis = closable instanceof BoundedInputStream ci ? ci
                : null;
        final int batchSize = context.getBatchSize(functionData.getOptions());
        return new TupleIterator()
        {
            TupleVector next;
            Schema schema;
            volatile boolean abort = false;

            Runnable abortListener = () ->
            {
                abort = true;
            };

            {
                context.getSession()
                        .registerAbortListener(abortListener);
            }

            @Override
            public TupleVector next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                TupleVector next = this.next;
                this.next = null;
                return next;
            }

            @Override
            public boolean hasNext()
            {
                try
                {
                    long time = System.nanoTime();
                    boolean hasNext = setNext();
                    nodeData.totalMillis += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                    nodeData.totalBytes = cis != null ? cis.getCount()
                            : 0;
                    return hasNext;
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Error reading CSV", e);
                }
            }

            @Override
            public void close()
            {
                context.getSession()
                        .unregisterAbortListener(abortListener);
                IOUtils.closeQuietly(closable, parser);
            }

            private boolean setNext() throws IOException
            {
                if (next != null)
                {
                    return true;
                }
                MutableValueVector[] vectors = null;
                int rowIndex = -1;
                int columnIndex = 0;
                while (parser.nextToken() != null)
                {
                    if (abort)
                    {
                        return false;
                    }

                    // Build the schema from the columns the parser found
                    if (schema == null)
                    {
                        CsvSchema csvSchema = parser.getSchema();
                        int size = csvSchema.size();
                        List<Column> columns = new ArrayList<>(size);
                        // TODO: schema. Now everything is a string
                        for (int i = 0; i < size; i++)
                        {
                            columns.add(Column.of(csvSchema.columnName(i), Type.String));
                        }
                        schema = new Schema(columns);
                    }

                    // Create builders for this batch
                    if (vectors == null)
                    {
                        int size = schema.getSize();
                        vectors = new MutableValueVector[size];
                        for (int i = 0; i < size; i++)
                        {
                            vectors[i] = context.getVectorFactory()
                                    .getMutableVector(ResolvedType.of(Type.String), batchSize);
                        }
                    }

                    JsonToken currentToken = parser.currentToken();

                    // Start next row
                    if (currentToken == JsonToken.START_OBJECT)
                    {
                        columnIndex = 0;
                        rowIndex++;
                        continue;
                    }
                    else if (currentToken == JsonToken.END_OBJECT)
                    {
                        // Make sure all vectors equals in size for each row
                        // this to adapt to missing values etc.
                        // Append nulls to all builders that is not in sync
                        int size = schema.getSize();
                        for (int i = 0; i < size; i++)
                        {
                            MutableValueVector vector = vectors[i];
                            if (vector.size() <= rowIndex)
                            {
                                vector.setNull(rowIndex);
                            }
                        }

                        if (rowIndex >= batchSize - 1)
                        {
                            break;
                        }
                        continue;
                    }
                    else if (currentToken == JsonToken.VALUE_STRING)
                    {
                        String value = parser.getValueAsString();
                        vectors[columnIndex].setString(rowIndex, UTF8String.from(value));
                        columnIndex++;
                    }
                }

                if (vectors == null)
                {
                    return false;
                }

                next = TupleVector.of(schema, vectors);
                return true;
            }
        };
    }

    Pair<Closeable, CsvParser> getParser(ValueVector value) throws IOException
    {
        if (value.type()
                .getType() == Type.Any)
        {
            // See if we have a wrapped reader/inputstram. Some catalogs can wrap a lazy reader for a file etc.
            Object obj = value.getAny(0);
            if (obj instanceof Reader r)
            {
                return Pair.of(r, (CsvParser) MAPPER.createParser(r));
            }
            else if (obj instanceof InputStream is)
            {
                BoundedInputStream cis = BoundedInputStream.builder()
                        .setInputStream(is)
                        .get();
                // Jackson determines encoding based on BOM
                return Pair.of(cis, (CsvParser) MAPPER.createParser(cis));
            }
        }

        return Pair.of(null, (CsvParser) MAPPER.createParser(value.valueAsString(0)));
    }

    static class CsvNodeData extends NodeData
    {
        long totalBytes;
        long totalMillis;
    }
}
