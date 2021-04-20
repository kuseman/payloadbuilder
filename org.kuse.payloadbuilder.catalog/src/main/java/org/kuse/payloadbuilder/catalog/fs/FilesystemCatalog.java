package org.kuse.payloadbuilder.catalog.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.io.FileUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Catalog providing file system functionality */
public class FilesystemCatalog extends Catalog
{
    private static final String[] COLUMNS = new String[] {
            "name",
            "path",
            "size",
            "exists",
            "creationTime",
            "lastAccessTime",
            "lastModifiedTime",
            "isDirectory"};

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

        @SuppressWarnings("unchecked")
        @Override
        public RowIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
        {
            Object obj = arguments.get(0).eval(context);
            if (obj == null)
            {
                return RowIterator.EMPTY;
            }
            String strPath = String.valueOf(obj);
            Path path = FileSystems.getDefault().getPath(strPath);
            return RowIterator.wrap(new SingletonIterator(ListFunction.fromPath(tableAlias, 0, path)));
        }
    }

    /** Contents function, outputs path contents as string */
    private static class ContentsFunction extends ScalarFunctionInfo
    {
        ContentsFunction(Catalog catalog)
        {
            super(catalog, "contents");
        }

        @Override
        public Object eval(ExecutionContext context, List<Expression> arguments)
        {
            Object p = arguments.get(0).eval(context);
            if (p == null)
            {
                return null;
            }
            String strPath = String.valueOf(p);
            try
            {
                Path path = FileSystems.getDefault().getPath(strPath);
                return new FSFileReader(path.toFile());
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error reading contents from: " + strPath, e);
            }
        }
    }

    /** File reader that reads whole file to string as fallback */
    static class FSFileReader extends BufferedReader
    {
        private final File file;
        FSFileReader(File file) throws FileNotFoundException
        {
            super(new FileReader(file));
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
        public RowIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
        {
            Object obj = arguments.get(0).eval(context);
            if (obj == null)
            {
                return RowIterator.EMPTY;
            }
            String path = String.valueOf(obj);
            boolean recursive = false;
            if (arguments.size() > 1)
            {
                recursive = (boolean) arguments.get(1).eval(context);
            }

            final Iterator<Path> it = getIterator(path, recursive);
            //CSOFF
            return new RowIterator()
            //CSON
            {
                private int pos;

                @Override
                public Row next()
                {
                    Path p = it.next();
                    return fromPath(tableAlias, pos++, p);
                }

                @Override
                public boolean hasNext()
                {
                    while (true)
                    {
                        try
                        {
                            return it.hasNext();
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
            };
        }

        static Row fromPath(TableAlias alias, int pos, Path path)
        {
            long creationTime = 0;
            long lastAccessTime = 0;
            long lastModifiedTime = 0;
            boolean isDirectory = false;
            long size = 0;
            try
            {
                BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
                creationTime = attr.creationTime().toMillis();
                lastAccessTime = attr.lastAccessTime().toMillis();
                lastModifiedTime = attr.lastModifiedTime().toMillis();
                isDirectory = attr.isDirectory();
                size = attr.size();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            Object[] values = new Object[] {
                    path.getFileName().toString(),
                    path.getParent() != null ? path.getParent().toString() : null,
                    size,
                    Files.exists(path),
                    creationTime,
                    lastAccessTime,
                    lastModifiedTime,
                    isDirectory
            };
            return Row.of(alias, pos, COLUMNS, values);
        }

        private Iterator<Path> getIterator(String strPath, boolean recursive)
        {
            try
            {
                Path path = FileSystems.getDefault().getPath(strPath);
                if (recursive)
                {
                    return Files.walk(path).iterator();
                }

                return Files.newDirectoryStream(path).iterator();
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error streaming directory " + strPath, e);
            }
        }
    }
}
