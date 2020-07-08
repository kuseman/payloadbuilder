package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.Select;
import com.viskan.payloadbuilder.parser.TableOption;

import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class ManualOperatorTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final QuerySession session = new QuerySession(new CatalogRegistry());

    @Test
    public void test()
    {
        String queryString = "select t.name, t.isDirectory, t.size, t2.name t2name from temp t inner join temp.temp2 t2 on t.name = t2.name ";
        Select select = parser.parseSelect(queryString);

        Catalog c = new Catalog("FS")
        {
            private DirectoryStream<Path> getStream(Path path)
            {
                try
                {
                    return Files.newDirectoryStream(path);
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Error streaming directory " + path, e);
                }
            }
            
            @Override
            public Operator getScanOperator(int nodeId, String catalogAlias, TableAlias tableAlias, List<TableOption> tableOptions)
            {
                return new Operator()
                {
                    @Override
                    public int getNodeId()
                    {
                        return 0;
                    }
                    
                    @Override
                    public Iterator<Row> open(ExecutionContext context)
                    {
                        String stringPath = tableAlias.getTable().getParts().stream().collect(joining("/", "C:/", ""));
                        Path path = Paths.get(stringPath);
                        final DirectoryStream<Path> directoryStream = getStream(path);
                        return new Iterator<Row>()
                        {
                            private final Iterator<Path> it = directoryStream.iterator();
                            private int pos = 0;

                            @Override
                            public Row next()
                            {
                                Path p = it.next();
                                String[] columns = tableAlias.getColumns();
                                Object[] values = new Object[columns.length];
                                for (int i = 0; i < columns.length; i++)
                                {
                                    if (columns[i].equals("size"))
                                    {
                                        long size = 0;
                                        try
                                        {
                                            size = Files.size(p);
                                        }
                                        catch (IOException e)
                                        {
                                        }
                                        values[i] = size;
                                    }
                                    else if (columns[i].equals("name"))
                                    {
                                        values[i] = p.getFileName();
                                    }
                                    else if (columns[i].equals("isDirectory"))
                                    {
                                        values[i] = Files.isDirectory(p);
                                    }
                                }

                                return Row.of(tableAlias, pos++, values);
                            }

                            @Override
                            public boolean hasNext()
                            {
                                return it.hasNext();
                            }
                        };
                    }

                    @Override
                    public String toString()
                    {
                        return tableAlias.getTable().toString();
                    }
                };
            }
        }; 
        
        session.setDefaultCatalog(c);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);

        System.out.println(pair.getKey().toString(1));

        ExecutionContext context = new ExecutionContext(session);
        JsonStringWriter writer = new JsonStringWriter();
        Iterator<Row> it = pair.getKey().open(context);
        while (it.hasNext())
        {
            context.setRow(it.next());
            pair.getValue().writeValue(writer, context);
            System.out.println(writer.getAndReset());
        }
    }
}
