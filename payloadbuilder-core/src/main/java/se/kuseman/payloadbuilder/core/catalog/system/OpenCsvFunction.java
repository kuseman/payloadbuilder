package se.kuseman.payloadbuilder.core.catalog.system;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
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
                // Jackson determines encoding based on BOM
                return Pair.of(is, (CsvParser) MAPPER.createParser(is));
            }
        }

        return Pair.of(null, (CsvParser) MAPPER.createParser(value.valueAsString(0)));
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, IDatasourceOptions options)
    {
        ValueVector value = arguments.get(0)
                .eval(context);

        if (value.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        CsvParser parser;
        Closeable closable;
        try
        {
            // TODO: schema input
            ValueVector vv = options.getOption(COLUMN_SEPARATOR, context);
            char columnSeparator = vv == null
                    || vv.isNull(0) ? CsvSchema.DEFAULT_COLUMN_SEPARATOR
                            : vv.getString(0)
                                    .toString()
                                    .charAt(0);

            vv = options.getOption(COLUMN_HEADERS, context);
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
                String[] columns = columnHeaders.split(String.valueOf(columnSeparator));
                Builder builder = CsvSchema.builder()
                        .setColumnSeparator(columnSeparator);
                for (String column : columns)
                {
                    builder.addColumn(column);
                }
                headerSchema = builder.build();
            }

            Pair<Closeable, CsvParser> pair = getParser(value);
            closable = pair.getKey();
            parser = pair.getValue();
            parser.setSchema(headerSchema);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error reading CSV", e);
        }

        final int batchSize = options.getBatchSize(context);
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
                    return setNext();
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
                IObjectVectorBuilder[] builders = null;
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
                    if (builders == null)
                    {
                        int size = schema.getSize();
                        builders = new IObjectVectorBuilder[size];
                        for (int i = 0; i < size; i++)
                        {
                            builders[i] = context.getVectorBuilderFactory()
                                    .getObjectVectorBuilder(ResolvedType.of(Type.String), batchSize);
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
                            IObjectVectorBuilder builder = builders[i];
                            if (builder.size() <= rowIndex)
                            {
                                builder.putNull();
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
                        builders[columnIndex].put(UTF8String.from(value));
                        columnIndex++;
                    }
                }

                if (builders == null)
                {
                    return false;
                }

                int size = schema.getSize();
                ValueVector[] vectors = new ValueVector[size];
                for (int i = 0; i < size; i++)
                {
                    vectors[i] = builders[i].build();
                }

                next = TupleVector.of(schema, vectors);
                return true;
            }
        };
    }
}
