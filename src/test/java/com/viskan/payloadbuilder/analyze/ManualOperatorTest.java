package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.OperatorFactory;
import com.viskan.payloadbuilder.operator.JsonStringWriter;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;

import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class ManualOperatorTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();

    @Test
    public void test()
    {

        String queryString = "select t.name, t.isDirectory, t.size, t2.name t2name from temp t inner join temp.temp2 t2 on t.name = t2.name ";
        Query query = parser.parseQuery(catalogRegistry, queryString);

        catalogRegistry.getDefault().setOperatorFactory(new OperatorFactory()
        {

            @Override
            public boolean requiresParents(QualifiedName qname)
            {
                return false;
            }

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
            public Operator create(QualifiedName qname, TableAlias tableAlias)
            {
                return new Operator()
                {
                    @Override
                    public Iterator<Row> open(OperatorContext context)
                    {
                        String stringPath = qname.getParts().stream().collect(joining("/", "C:/", ""));
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
                        return qname.toString();
                    }
                };
            }
        });

        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);

        System.out.println(pair.getKey().toString(1));

        OperatorContext context = new OperatorContext();
        JsonStringWriter writer = new JsonStringWriter();
        Iterator<Row> it = pair.getKey().open(context);
        while (it.hasNext())
        {
            pair.getValue().writeValue(writer, context, it.next());
            System.out.println(writer.getAndReset());
        }
    }
}
