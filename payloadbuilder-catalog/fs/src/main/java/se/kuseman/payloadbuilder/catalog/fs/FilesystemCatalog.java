package se.kuseman.payloadbuilder.catalog.fs;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.upperCase;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.InsertIntoData;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.SelectIntoData;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext.VectorWriterFormat;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** Catalog providing file system functionality */
public class FilesystemCatalog extends Catalog
{
    private static final String NAME = "Filesystem";

    /** Table option to force file format if filename is not enough. */
    static final QualifiedName FORMAT = QualifiedName.of("format");

    /** Construct a new file system catalog */
    public FilesystemCatalog()
    {
        super(NAME);
        registerFunction(new ListFunction());
        registerFunction(new FileFunction());
        registerFunction(new ContentsFunction());
    }

    @Override
    public TableSchema getTableSchema(IExecutionContext context, String catalogAlias, QualifiedName table, List<Option> options)
    {
        if (table.size() != 1)
        {
            throw new CompileException("Tables qualifiers for " + NAME + " only supports one part.");
        }

        ReadResolveResult resolveResult = resolveForRead(context, catalogAlias, table, options);

        // NOTE! We send in the filename as the argument here which is wrong but ok for the get schema function
        // that should never have the actual contents anyways.
        Schema schema = resolveResult.tableFunction.getSchema(context, catalogAlias, List.of(context.getExpressionFactory()
                .createStringExpression(UTF8String.from(resolveResult.filename))), resolveResult.options);
        return new TableSchema(schema);
    }

    @Override
    public void dropTable(IQuerySession session, String catalogAlias, QualifiedName table, boolean lenient)
    {
        if (table.size() != 1)
        {
            throw new IllegalArgumentException("Tables qualifiers for " + NAME + " only supports one part.");
        }

        String filename = getFilename(session, catalogAlias, table);
        Path path = Paths.get(filename);
        boolean exists = Files.exists(path);
        if (lenient
                && !exists)
        {
            return;
        }
        else if (!exists
                || !Files.isRegularFile(path))
        {
            throw new IllegalArgumentException("Path does not exists or is not a file: " + path);
        }

        FileUtils.deleteQuietly(path.toFile());
    }

    @Override
    public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        if (table.size() != 1)
        {
            throw new IllegalArgumentException("Tables qualifiers for " + NAME + " only supports one part.");
        }

        return new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context)
            {
                ReadResolveResult resolveResult = resolveForRead(context, catalogAlias, table, data.getOptions());

                InputStream fis = getFileInputStream(resolveResult.filename);
                IExpression arg = new IExpression()
                {
                    @Override
                    public ResolvedType getType()
                    {
                        return ResolvedType.ANY;
                    }

                    @Override
                    public ValueVector eval(TupleVector input, IExecutionContext context)
                    {
                        return ValueVector.literalAny(1, fis);
                    }

                    @Override
                    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
                    {
                        throw new RuntimeException("Cannot be visited");
                    }
                };

                TupleIterator iterator = resolveResult.tableFunction.execute(context, catalogAlias, List.of(arg), new FunctionData(-1, resolveResult.options));
                return new TupleIterator()
                {
                    @Override
                    public TupleVector next()
                    {
                        return iterator.next();
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }

                    @Override
                    public void close()
                    {
                        iterator.close();
                        try
                        {
                            fis.close();
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException("Error closing file input stream", e);
                        }
                    }
                };

            }
        };
    }

    @Override
    public IDatasink getSelectIntoSink(IQuerySession session, String catalogAlias, QualifiedName table, SelectIntoData data)
    {
        return new InsertSink(this, catalogAlias, table, data.getOptions(), emptyList());
    }

    @Override
    public IDatasink getInsertIntoSink(IQuerySession session, String catalogAlias, QualifiedName table, InsertIntoData data)
    {
        return new InsertSink(this, catalogAlias, table, data.getOptions(), data.getInsertColumns());
    }

    private InputStream getFileInputStream(String filename)
    {
        try
        {
            return new BufferedInputStream(Files.newInputStream(Path.of(filename)));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error creating file input stream", e);
        }
    }

    record ReadResolveResult(String filename, Format format, TableFunctionInfo tableFunction, List<Option> options)
    {
    }

    record WriteResolveResult(String filename, IExecutionContext.VectorWriterFormat format, List<Option> options)
    {
    }

    private String getFilename(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        String filename = table.getFirst();
        // First try to see if there is a catalog property for the table (file)
        // this to be able to use files without hard coding
        ValueVector vector = session.getCatalogProperty(catalogAlias, filename);
        if (vector != null
                && !vector.isNull(0))
        {
            filename = vector.valueAsString(0);
        }
        return filename;
    }

    WriteResolveResult resolveForWrite(IExecutionContext context, String catalogAlias, QualifiedName table, List<Option> options)
    {
        String filename = getFilename(context.getSession(), catalogAlias, table);
        IExecutionContext.VectorWriterFormat type;
        ValueVector format = context.getOption(FORMAT, options);
        if (format != null
                && !format.isNull(0))
        {
            type = IExecutionContext.VectorWriterFormat.valueOf(upperCase(format.valueAsString(0)));
        }
        else
        {
            String extension = FilenameUtils.getExtension(filename);
            if ("CSV".equalsIgnoreCase(extension))
            {
                type = VectorWriterFormat.CSV;
            }
            else if ("JSON".equalsIgnoreCase(extension))
            {
                type = VectorWriterFormat.JSON;
            }
            else if ("TXT".equalsIgnoreCase(extension))
            {
                type = VectorWriterFormat.TEXT;
            }
            else
            {
                // Write as text as fallback
                type = VectorWriterFormat.TEXT;
            }
        }

        return new WriteResolveResult(filename, type, options);
    }

    ReadResolveResult resolveForRead(IExecutionContext context, String catalogAlias, QualifiedName table, List<Option> options)
    {
        String filename = getFilename(context.getSession(), catalogAlias, table);
        Format type;
        ValueVector format = context.getOption(FORMAT, options);
        if (format != null
                && !format.isNull(0))
        {
            type = Format.valueOf(upperCase(format.valueAsString(0)));
        }
        else
        {
            type = Format.from(FilenameUtils.getExtension(filename));
            if (type == null)
            {
                // Fallback to CSV with a value column, this will return the file row by row
                type = Format.CSV;
                options = new ArrayList<>(options);
                // NOTE! This is a hard coded string from opencsv function that is kind of magic here but will do.
                options.add(0, new Option(QualifiedName.of("columnHeaders"), context.getExpressionFactory()
                        .createStringExpression(UTF8String.from("value"))));
                options.add(0, new Option(QualifiedName.of("columnSeparator"), context.getExpressionFactory()
                        .createStringExpression(UTF8String.from("\0"))));
            }
        }

        TableFunctionInfo tableFunction = context.getSession()
                .getSystemCatalog()
                .getTableFunction(type.systemFunction);

        return new ReadResolveResult(filename, type, tableFunction, options);
    }

    enum Format
    {
        JSON("OPENJSON"),
        CSV("OPENCSV"),
        XML("OPENXML");

        private static final Format[] VALUES = Format.values();
        private final String systemFunction;

        Format(String systemFunction)
        {
            this.systemFunction = requireNonNull(systemFunction);
        }

        private static Format from(String value)
        {
            return Arrays.stream(VALUES)
                    .filter(v -> v.name()
                            .equalsIgnoreCase(value))
                    .findFirst()
                    .orElse(null);
        }
    }
}
