package se.kuseman.payloadbuilder.catalog.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.EpochDateTime;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Catalog providing file system functionality */
public class FilesystemCatalog extends Catalog
{
    //@formatter:off
    private static final Schema SCHEMA = Schema.of(
            Column.of("name", ResolvedType.of(Type.String)),
            Column.of("path", ResolvedType.of(Type.String)),
            Column.of("size", ResolvedType.of(Type.Long)),
            Column.of("exists", ResolvedType.of(Type.Boolean)),
            Column.of("creationTime", ResolvedType.of(Type.DateTime)),
            Column.of("lastAccessTime", ResolvedType.of(Type.DateTime)),
            Column.of("lastModifiedTime", ResolvedType.of(Type.DateTime)),
            Column.of("isDirectory", ResolvedType.of(Type.Boolean)));
    //@formatter:on

    /** Construct a new file system catalog */
    public FilesystemCatalog()
    {
        super("Filesystem");
        registerFunction(new ListFunction(this));
        registerFunction(new FileFunction(this));
        registerFunction(new ContentsFunction(this));
    }

    /** File function */
    private static class FileFunction extends TableFunctionInfo
    {
        FileFunction(Catalog catalog)
        {
            super(catalog, "file");
        }

        @Override
        public Schema getSchema(List<? extends IExpression> arguments)
        {
            return SCHEMA;
        }

        @Override
        public int arity()
        {
            return 1;
        }

        @Override
        public TupleIterator execute(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments, IDatasourceOptions options)
        {
            Object obj = arguments.get(0)
                    .eval(context);
            if (obj == null)
            {
                return TupleIterator.EMPTY;
            }
            String strPath = String.valueOf(obj);
            Path path = FileSystems.getDefault()
                    .getPath(strPath);
            Object[] values = ListFunction.fromPath(path);
            return TupleIterator.singleton(new ObjectTupleVector(SCHEMA, 1, (row, col) -> values[col]));
        }
    }

    /** Contents function, outputs path contents as string */
    private static class ContentsFunction extends ScalarFunctionInfo
    {
        ContentsFunction(Catalog catalog)
        {
            super(catalog, "contents", FunctionType.SCALAR);
        }

        @Override
        public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
        {
            final ValueVector value = arguments.get(0)
                    .eval(input, context);
            final int size = input.getRowCount();
            final boolean nullable = value.isNullable();
            return new ValueVector()
            {
                @Override
                public ResolvedType type()
                {
                    return ResolvedType.of(Type.Any);
                }

                @Override
                public int size()
                {
                    return size;
                }

                @Override
                public boolean isNullable()
                {
                    return nullable;
                }

                @Override
                public boolean isNull(int row)
                {
                    return value.isNull(row);
                }

                @Override
                public Object getValue(int row)
                {
                    String strPath = String.valueOf(value.getString(row)
                            .toString());
                    try
                    {
                        Path path = FileSystems.getDefault()
                                .getPath(strPath);
                        return new FSFileReader(path.toFile());
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("Error reading contents from: " + strPath, e);
                    }
                }
            };
        }
    }

    /** File reader that reads whole file to string as fallback */
    static class FSFileReader extends BufferedReader
    {
        private final File file;

        FSFileReader(File file) throws FileNotFoundException
        {
            super(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
            this.file = file;
        }

        @Override
        public String toString()
        {
            // Fallback to read the file to string for operators and functions not supporting a reader
            try
            {
                return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error reading file contents to string", e);
            }
        }
    }

    /** List function. Traverses a path */
    private static class ListFunction extends TableFunctionInfo
    {
        ListFunction(Catalog catalog)
        {
            super(catalog, "list");
        }

        @Override
        public Schema getSchema(List<? extends IExpression> arguments)
        {
            return SCHEMA;
        }

        @Override
        public TupleIterator execute(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments, IDatasourceOptions options)
        {
            ValueVector value = arguments.get(0)
                    .eval(context);
            if (value.isNullable()
                    && value.isNull(0))
            {
                return TupleIterator.EMPTY;
            }

            String path = String.valueOf(value.getString(0)
                    .toString());

            boolean recursive = false;
            if (arguments.size() > 1)
            {
                value = arguments.get(1)
                        .eval(context);
                // We treat null as false here
                recursive = value.getPredicateBoolean(0);
            }

            final Iterator<Path> it = getIterator(path, recursive);

            // TODO: batch size option
            final int batchSize = 500;

            return new TupleIterator()
            {
                TupleVector next;
                int count = 0;

                @Override
                public TupleVector next()
                {
                    if (next == null)
                    {
                        throw new NoSuchElementException();
                    }
                    TupleVector result = next;
                    next = null;
                    count = 0;
                    return result;
                }

                @Override
                public boolean hasNext()
                {
                    return setNext();
                }

                private boolean setNext()
                {
                    while (next == null)
                    {
                        List<Object[]> batch = new ArrayList<>(batchSize);
                        while (it.hasNext()
                                && count < batchSize)
                        {
                            batch.add(fromPath(it.next()));
                            count++;
                        }

                        if (batch.isEmpty())
                        {
                            return false;
                        }

                        next = new TupleVector()
                        {
                            @Override
                            public Schema getSchema()
                            {
                                return SCHEMA;
                            }

                            @Override
                            public int getRowCount()
                            {
                                return batch.size();
                            }

                            @Override
                            public ValueVector getColumn(int column)
                            {
                                return new ValueVector()
                                {
                                    @Override
                                    public ResolvedType type()
                                    {
                                        return SCHEMA.getColumns()
                                                .get(column)
                                                .getType();
                                    }

                                    @Override
                                    public int size()
                                    {
                                        return batch.size();
                                    }

                                    @Override
                                    public boolean isNull(int row)
                                    {
                                        return batch.get(row)[column] == null;
                                    }

                                    @Override
                                    public Object getValue(int row)
                                    {
                                        return batch.get(row)[column];
                                    }
                                };
                            }
                        };

                        // next = new ObjectTupleVector(SCHEMA, batch.size(), (row, col) ->
                        // {
                        // Object[] values = fromPath(batch.get(row));
                        // return
                        // });

                        // next = new TupleVector()
                        // {
                        // @Override
                        // public Schema getSchema()
                        // {
                        // return SCHEMA;
                        // }
                        //
                        // @Override
                        // public int getRowCount()
                        // {
                        // return batch.size();
                        // }
                        //
                        // @Override
                        // public ValueVector getColumn(int column)
                        // {
                        // // TODO Auto-generated method stub
                        // return null;
                        // }
                        // };
                    }
                    return true;
                }
            };
        }
        //
        // @Override
        // public TupleIterator open(IExecutionContext context, String catalogAlias, TableAlias tableAlias, List<? extends IExpression> arguments)
        // {
        // Object obj = arguments.get(0)
        // .eval(context);
        // if (obj == null)
        // {
        // return TupleIterator.EMPTY;
        // }
        // String path = String.valueOf(obj);
        // boolean recursive = false;
        // if (arguments.size() > 1)
        // {
        // recursive = (boolean) arguments.get(1)
        // .eval(context);
        // }
        //
        // final Iterator<Path> it = getIterator(path, recursive);
        // // CSOFF
        // return new TupleIterator()
        // // CSON
        // {
        // @Override
        // public Row next()
        // {
        // Path p = it.next();
        // return fromPath(tableAlias, p);
        // }
        //
        // @Override
        // public boolean hasNext()
        // {
        // while (true)
        // {
        // try
        // {
        // return it.hasNext();
        // }
        // catch (Exception e)
        // {
        // e.printStackTrace();
        // }
        // }
        // }
        // };
        // }

        static Object[] fromPath(Path path)
        {
            long creationTime = 0;
            long lastAccessTime = 0;
            long lastModifiedTime = 0;
            boolean isDirectory = false;
            long size = 0;
            try
            {
                BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
                creationTime = attr.creationTime()
                        .toMillis();
                lastAccessTime = attr.lastAccessTime()
                        .toMillis();
                lastModifiedTime = attr.lastModifiedTime()
                        .toMillis();
                isDirectory = attr.isDirectory();
                size = attr.size();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            //@formatter:off
            Object[] values = new Object[] {
                    UTF8String.from(path.getFileName().toString()),
                    path.getParent() != null ? UTF8String.from(path.getParent().toString()): null,
                    size,
                    Files.exists(path),
                    EpochDateTime.from(creationTime),
                    EpochDateTime.from(lastAccessTime),
                    EpochDateTime.from(lastModifiedTime),
                    isDirectory };
            //@formatter:on
            return values;
        }

        private Iterator<Path> getIterator(String strPath, boolean recursive)
        {
            try
            {
                Path path = FileSystems.getDefault()
                        .getPath(strPath);
                if (recursive)
                {
                    return Files.walk(path)
                            .iterator();
                }

                return Files.newDirectoryStream(path)
                        .iterator();
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error streaming directory " + strPath, e);
            }
        }
    }
}
