package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.io.Closeable;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.namespace.QName;
import javax.xml.stream.EventFilter;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

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
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IVectorBuilderFactory;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** TVF for opening an xml argument and transform into a {@link TupleIterator} */
class OpenXmlFunction extends TableFunctionInfo
{
    static final QualifiedName XMLPATH = QualifiedName.of("xmlpath");
    static final QualifiedName XMLCOLUMNS = QualifiedName.of("xmlcolumns");
    private static final QName NIL = new QName("http://www.w3.org/2001/XMLSchema-instance", "nil", "xsi");

    public OpenXmlFunction()
    {
        super("openxml");
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    private Pair<Closeable, XMLEventReader> getXmlReader(XMLInputFactory xmlInputFactory, ValueVector value) throws XMLStreamException
    {
        if (value.type()
                .getType() == Type.Any)
        {
            // See if we have a wrapped reader/inputstram. Some catalogs can wrap a lazy reader for a file etc.
            Object obj = value.getAny(0);
            if (obj instanceof Reader r)
            {
                return Pair.of(r, xmlInputFactory.createXMLEventReader(r));
            }
            else if (obj instanceof InputStream is)
            {
                return Pair.of(is, xmlInputFactory.createXMLEventReader(is, StandardCharsets.UTF_8.name()));
            }
        }

        return Pair.of(null, xmlInputFactory.createXMLEventReader(new StringReader(value.valueAsString(0))));
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

        Closeable closable;
        XMLEventReader xmlReader;
        String xmlPath;
        String[] columnsOption;
        try
        {
            // TODO: schema input
            ValueVector vv = options.getOption(XMLPATH, context);
            xmlPath = vv == null
                    || vv.isNull(0) ? ""
                            : vv.getString(0)
                                    .toString();

            vv = options.getOption(XMLCOLUMNS, context);
            columnsOption = vv == null
                    || vv.isNull(0) ? null
                            : StringUtils.split(vv.getString(0)
                                    .toString(), ',');

            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            Pair<Closeable, XMLEventReader> pair = getXmlReader(xmlInputFactory, value);
            closable = pair.getKey();
            XMLEventReader rawReader = pair.getValue();
            xmlReader = xmlInputFactory.createFilteredReader(rawReader, new EventFilter()
            {
                @Override
                public boolean accept(XMLEvent event)
                {
                    if (event.isCharacters())
                    {
                        Characters characters = event.asCharacters();
                        return !characters.isWhiteSpace();
                    }
                    return true;
                }
            });
        }
        catch (XMLStreamException e)
        {
            throw new RuntimeException("Error reading XML", e);
        }

        final int batchSize = options.getBatchSize(context);
        final String[] xmlPathParts = StringUtils.split(xmlPath, '/');
        final String rowElementName = xmlPathParts.length > 0 ? xmlPathParts[xmlPathParts.length - 1]
                : "";
        final Set<String> schemaColumns = columnsOption != null ? Arrays.stream(columnsOption)
                .collect(Collectors.toCollection(LinkedHashSet::new))
                : null;
        return new TupleIterator()
        {
            TupleVector next;
            StringBuilder xmlBuilder = new StringBuilder();
            Map<String, XmlColumn> columnByName = new LinkedHashMap<>();
            int actualColumnCount;
            boolean pathFound = xmlPathParts == null
                    || xmlPathParts.length <= 1;
            boolean endFound = false;
            volatile boolean abort = false;

            Runnable abortListener = () ->
            {
                abort = true;
            };

            {
                context.getSession()
                        .registerAbortListener(abortListener);
            }

            /** Holder for each column. It's name along with the vector builder */
            static class XmlColumn
            {
                final String name;
                IObjectVectorBuilder builder;
                List<String> value;

                XmlColumn(String name)
                {
                    this.name = name;
                }

                void appendRowValue(String value)
                {
                    if (this.value == null)
                    {
                        this.value = singletonList(value);
                    }
                    else if (this.value.size() == 1)
                    {
                        List<String> tmp = new ArrayList<>();
                        tmp.add(this.value.get(0));
                        tmp.add(value);
                        this.value = tmp;
                    }
                    else
                    {
                        this.value.add(value);
                    }
                }

                void pushRow(int currentRowCount, IVectorBuilderFactory vectorFactory, int estimatedSize)
                {
                    // Nothing
                    if (builder == null
                            && value == null)
                    {
                        return;
                    }
                    // Create a builder
                    else if (builder == null)
                    {
                        builder = vectorFactory.getObjectVectorBuilder(ResolvedType.of(Type.Any), estimatedSize);

                        // Pad nulls if needed, this happens if we add a new column on a non first row
                        if (currentRowCount > 0)
                        {
                            for (int i = 0; i < currentRowCount; i++)
                            {
                                builder.putNull();
                            }
                        }
                    }
                    // Builder but no value => align builder to have the same size as all columns
                    else if (value == null)
                    {
                        builder.putNull();
                        return;
                    }

                    if (value.size() == 1)
                    {
                        builder.put(value.get(0));
                    }
                    // Multi value => create array
                    else
                    {
                        builder.put(VectorUtils.convertToValueVector(value));
                    }
                }

                ValueVector build()
                {
                    if (builder != null)
                    {
                        return builder.build();
                    }
                    return null;
                }
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
                if (next != null)
                {
                    return true;
                }
                else if (endFound)
                {
                    return false;
                }

                try
                {
                    return setNext();
                }
                catch (XMLStreamException e)
                {
                    throw new RuntimeException("Error reading XML", e);
                }
            }

            @Override
            public void close()
            {
                context.getSession()
                        .unregisterAbortListener(abortListener);
                // XMLEventReader doesn't close the underlaying stream
                // so we close that once explicitly
                IOUtils.closeQuietly(closable);
                try
                {
                    xmlReader.close();
                }
                catch (XMLStreamException e)
                {
                    // SWALLOW
                }
            }

            private boolean setNext() throws XMLStreamException
            {
                // Stream until we find the second to last element in path
                if (!pathFound)
                {
                    int length = xmlPathParts.length;
                    int foundCount = 0;
                    String elementToMatch = xmlPathParts[foundCount];

                    while (xmlReader.hasNext())
                    {
                        XMLEvent event = xmlReader.nextEvent();

                        if (event.isStartElement())
                        {
                            if (elementToMatch.equalsIgnoreCase(event.asStartElement()
                                    .getName()
                                    .getLocalPart()))
                            {
                                foundCount++;
                                if (foundCount == length - 1)
                                {
                                    pathFound = true;
                                    break;
                                }
                                elementToMatch = xmlPathParts[foundCount];
                            }
                        }
                    }
                    // path was not found then we have no more rows
                    if (!pathFound)
                    {
                        return false;
                    }
                }

                // Reset the column builders before each batch
                for (XmlColumn column : columnByName.values())
                {
                    column.builder = null;
                }
                actualColumnCount = 0;
                int rowCount = 0;

                boolean insideRowElement = false;

                XMLEvent event = null;
                while (xmlReader.hasNext())
                {
                    if (abort)
                    {
                        return false;
                    }

                    // Don't read past the row element name if blank
                    if (!(rowElementName.isBlank()
                            && insideRowElement))
                    {
                        event = xmlReader.nextEvent();
                    }
                    // Not inside a row, stream until we find the start element
                    if (!insideRowElement)
                    {
                        if (event.isStartElement())
                        {
                            StartElement element = event.asStartElement();
                            if (rowElementName.isBlank()
                                    || rowElementName.equalsIgnoreCase(element.getName()
                                            .getLocalPart()))
                            {
                                insideRowElement = true;
                            }
                            else
                            {
                                // Loop past this whole sub element if this wasn't our target
                                streamPastElement(event, xmlReader);
                            }

                        }
                        // We are done
                        else if (event.isEndElement()
                                || event.isEndDocument())
                        {
                            endFound = true;
                            break;
                        }
                    }
                    else
                    {
                        rowCount += readRow(event, rowCount, rowElementName);
                        insideRowElement = false;
                        if (rowCount >= batchSize)
                        {
                            break;
                        }
                    }
                }

                if (rowCount == 0)
                {
                    return false;
                }

                // Build schema and vector from all builders
                List<Column> columns = new ArrayList<>(actualColumnCount);
                List<ValueVector> vectors = new ArrayList<>(actualColumnCount);

                /* If we have specified columns then we must add those in order to follow schema */
                if (schemaColumns != null)
                {
                    for (String column : schemaColumns)
                    {
                        columns.add(Column.of(column, Column.Type.Any));
                        XmlColumn xmlColumn = columnByName.get(column);
                        if (xmlColumn == null)
                        {
                            // Add null column if we don't have and XML rows for current
                            vectors.add(ValueVector.literalNull(ResolvedType.of(Column.Type.Any), rowCount));
                        }
                        else
                        {
                            ValueVector vector = xmlColumn.build();
                            if (vector != null)
                            {
                                vectors.add(vector);
                            }
                        }
                    }
                }
                else
                {
                    for (XmlColumn xmlColumn : columnByName.values())
                    {
                        ValueVector vector = xmlColumn.build();
                        if (vector != null)
                        {
                            columns.add(Column.of(xmlColumn.name, Column.Type.Any));
                            vectors.add(vector);
                        }
                    }
                }

                if (vectors.isEmpty())
                {
                    return false;
                }

                next = TupleVector.of(new Schema(columns), vectors);
                return true;
            }

            private void streamPastElement(XMLEvent event, XMLEventReader xmlReader) throws XMLStreamException
            {
                int nestCount = 0;
                while (!(event.isEndElement()
                        && nestCount == 0))
                {
                    if (event.isStartElement())
                    {
                        nestCount++;
                    }
                    event = xmlReader.nextEvent();
                    if (event.isEndElement())
                    {
                        nestCount--;
                    }
                }
            }

            /**
             * Routine that reads a row form current position. Reads data until an end element if found with the provided rowElementName
             */
            private int readRow(XMLEvent event, int currentRowCount, String rowElementName) throws XMLStreamException
            {
                // Clear the values before each row
                for (XmlColumn column : columnByName.values())
                {
                    column.value = null;
                }

                // A pointer to a non element, then we simple add that value to the a column named as the pointer
                if (!event.isStartElement())
                {
                    XmlColumn column = getColumn(rowElementName, currentRowCount);
                    extractContents(column, null, event);
                    column.pushRow(currentRowCount, context.getVectorBuilderFactory(), batchSize);
                    event = xmlReader.nextEvent();
                    return 1;
                }

                boolean columnsFound = false;
                StartElement element = null;
                // Stream element by element
                while (!(event.isEndElement()
                        || event.isEndDocument()))
                {
                    if (abort)
                    {
                        return 0;
                    }

                    if (element == null)
                    {
                        element = event.asStartElement();
                    }

                    String name = element.getName()
                            .getLocalPart();

                    if (schemaColumns != null
                            && !schemaColumns.contains(name))
                    {
                        streamPastElement(event, xmlReader);
                        event = xmlReader.nextEvent();
                        element = null;
                        continue;
                    }

                    columnsFound = true;

                    // CSOFF
                    XmlColumn column = getColumn(name, currentRowCount);
                    // CSON
                    Iterator<Attribute> attributes = element.getAttributes();
                    /* Each attribute gets it's own column who's name is combined from element name + attribute name */
                    boolean isNull = false;
                    while (attributes.hasNext())
                    {
                        Attribute attribute = attributes.next();
                        String value = attribute.getValue();

                        if (NIL.equals(attribute.getName())
                                && "true".equalsIgnoreCase(value))
                        {
                            isNull = true;
                            break;
                        }

                        String attributeName = name + "_"
                                               + attribute.getName()
                                                       .getLocalPart();

                        XmlColumn attributeColumn = getColumn(attributeName, currentRowCount);
                        attributeColumn.appendRowValue(value);
                    }

                    // Next event => either value in element or nested elements
                    event = xmlReader.nextEvent();
                    if (!isNull)
                    {
                        extractContents(column, element, event);
                    }

                    // Move to next field
                    event = xmlReader.nextEvent();
                    element = null;
                }

                if (!columnsFound)
                {
                    return 0;
                }

                // Push the values into builders that we found for current row
                for (XmlColumn column : columnByName.values())
                {
                    column.pushRow(currentRowCount, context.getVectorBuilderFactory(), batchSize);
                }

                return 1;
            }

            private void extractContents(XmlColumn column, StartElement element, XMLEvent event) throws XMLStreamException
            {
                if (event.isCharacters())
                {
                    // If an element contains &amp; we have more than one event so concat all consecutive characters events
                    xmlBuilder.setLength(0);
                    while (event.isCharacters())
                    {
                        xmlBuilder.append(event.asCharacters()
                                .getData());
                        event = xmlReader.nextEvent();
                    }
                    column.appendRowValue(xmlBuilder.toString());
                }
                else if (event.isEndElement())
                {
                    return;
                }
                else
                {
                    int nestCount = 0;
                    xmlBuilder.setLength(0);

                    // For nested element values we include the whole sub xml in the column
                    xmlBuilder.append(element.toString());

                    while (!(event.isEndElement()
                            && nestCount == 0))
                    {
                        xmlBuilder.append(event.toString());

                        if (event.isStartElement())
                        {
                            nestCount++;
                        }
                        else if (event.isEndElement())
                        {
                            nestCount--;
                        }

                        event = xmlReader.nextEvent();
                    }

                    xmlBuilder.append(event.toString());

                    column.appendRowValue(xmlBuilder.toString());
                }
            }

            private XmlColumn getColumn(String name, int currentRowCount)
            {
                XmlColumn column = columnByName.computeIfAbsent(schemaColumns != null ? name
                        : name.toLowerCase(), k -> new XmlColumn(name));
                if (column.value == null)
                {
                    actualColumnCount++;
                }
                return column;
            }
        };
    }
}
