package se.kuseman.payloadbuilder.catalog.fs;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** List function. Traverses a path */
class ListFunction extends TableFunctionInfo
{
    ListFunction()
    {
        super("list");
    }

    @Override
    public Schema getSchema(List<IExpression> arguments)
    {
        return FileFunction.SCHEMA;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
    {
        ValueVector value = arguments.get(0)
                .eval(context);
        if (value.isNull(0))
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

        final int batchSize = context.getBatchSize(data.getOptions());

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
                            return FileFunction.SCHEMA;
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
                                    return FileFunction.SCHEMA.getColumns()
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
                                public Object getAny(int row)
                                {
                                    return batch.get(row)[column];
                                }
                            };
                        }
                    };
                }
                return true;
            }
        };
    }

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