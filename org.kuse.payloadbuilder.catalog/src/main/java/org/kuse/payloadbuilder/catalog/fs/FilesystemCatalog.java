package org.kuse.payloadbuilder.catalog.fs;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Catalog providing file system functionality */
class FilesystemCatalog extends Catalog
{
    private static final String[] COLUMNS = new String[] {"name", "path", "size", "isDirectory", "_path"};

    FilesystemCatalog()
    {
        super("Filesystem");
        registerFunction(new ListFunction(this));
        registerFunction(new ContentsFunction(this));
    }

    /** Contents function, outputs path contents as string */
    private static class ContentsFunction extends ScalarFunctionInfo
    {
        public ContentsFunction(Catalog catalog)
        {
            super(catalog, "contents", Type.SCALAR);
        }

        @Override
        public Object eval(ExecutionContext context, List<Expression> arguments)
        {
            Object p = arguments.get(0).eval(context);
            if (!(p instanceof Path) || Files.isDirectory((Path) p))
            {
                return null;
            }
            try
            {
                return FileUtils.readFileToString(((Path) p).toFile());
            }
            catch (IOException e)
            {
                return null;
            }
        }
    }

    /** List function. Traverses a path */
    private static class ListFunction extends TableFunctionInfo
    {
        public ListFunction(Catalog catalog)
        {
            super(catalog, "list", Type.TABLE);
        }

        @Override
        public Iterator<Row> open(ExecutionContext context, TableAlias tableAlias, List<Object> arguments)
        {
            tableAlias.setColumns(COLUMNS);
            String path = String.valueOf(arguments.get(0));
            boolean recursive = false;
            if (arguments.size() > 1)
            {
                recursive = (boolean) arguments.get(1);
            }

            final Iterator<Path> it = getIterator(path, recursive);
            return new Iterator<Row>()
            {
                private int pos = 0;

                @Override
                public Row next()
                {
                    Path p = it.next();
                    Object[] values = new Object[] {
                            p.getFileName(),
                            p.getParent().toString(),
                            getSize(p),
                            Files.isDirectory(p),
                            p
                    };
                    return Row.of(tableAlias, pos++, values);
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
                        }
                    }
                }
            };
        }

        private long getSize(Path path)
        {
            try
            {
                return Files.size(path);
            }
            catch (IOException e)
            {
            }
            return 0;
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
