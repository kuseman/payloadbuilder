package se.kuseman.payloadbuilder.catalog.fs;

import static java.util.Objects.requireNonNull;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.VectorWriter;
import se.kuseman.payloadbuilder.catalog.fs.FilesystemCatalog.WriteResolveResult;

/** Insert sink for FS catalog. */
class InsertSink implements IDatasink
{
    /** We always append or create even if we break contract with select into / insert into. This because for easier usability. */
    private static final OpenOption[] OPENOPTIONS = new OpenOption[] { StandardOpenOption.APPEND, StandardOpenOption.CREATE };

    private final FilesystemCatalog catalog;
    private final String catalogAlias;
    private final QualifiedName table;
    private final List<Option> options;
    private final List<String> insertColumns;

    InsertSink(FilesystemCatalog catalog, String catalogAlias, QualifiedName table, List<Option> options, List<String> insertColumns)
    {
        this.catalog = requireNonNull(catalog);
        this.catalogAlias = requireNonNull(catalogAlias);
        this.table = requireNonNull(table);
        this.options = requireNonNull(options);
        this.insertColumns = requireNonNull(insertColumns);
    }

    @Override
    public void execute(IExecutionContext context, TupleIterator input)
    {
        WriteResolveResult resolveResult = catalog.resolveForWrite(context, catalogAlias, table, options);

        //@formatter:off
        try (OutputStream os = Files.newOutputStream(Path.of(resolveResult.filename()), OPENOPTIONS);
                VectorWriter vectorWriter = context.getVectorWriter(resolveResult.format(), os, options))
        //@formatter:on
        {
            while (input.hasNext())
            {
                TupleVector next = getVector(input.next());
                vectorWriter.write(next);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error writing to file", e);
        }
        finally
        {
            input.close();
        }
    }

    private TupleVector getVector(TupleVector vector)
    {
        if (insertColumns.isEmpty())
        {
            return vector;
        }

        // Change name of schema
        List<Column> columns = vector.getSchema()
                .getColumns();
        final Schema schema = new Schema(IntStream.range(0, columns.size())
                .mapToObj(i -> Column.of(insertColumns.get(i), columns.get(i)
                        .getType(),
                        columns.get(i)
                                .getMetaData()))
                .toList());

        return new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return vector.getRowCount();
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return vector.getColumn(column);
            }
        };
    }
}
