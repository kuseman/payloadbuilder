package com.viskan.payloadbuilder.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;

import java.util.List;
import java.util.Objects;

public class QualifiedName
{
    private final List<String> parts;
    private final String catalog;

    public QualifiedName(String catalog, List<String> parts)
    {
        this.catalog = catalog;
        this.parts = unmodifiableList(requireNonNull(parts));
    }

    public String getCatalog()
    {
        return catalog;
    }

    public List<String> getParts()
    {
        return parts;
    }

    /**
     * Returns the aliss if any. An alias exist if there are more than one part in the name
     */
    public String getAlias()
    {
        if (parts.size() == 1)
        {
            return null;
        }

        return parts.get(0);
    }

    /** Get the last part of the qualified name */
    public String getLast()
    {
        return parts.get(parts.size() - 1);
    }

    /** Get the first part in qualified name */
    public String getFirst()
    {
        return parts.get(0);
    }

    /** Extracts a new qualified name from this instance with parts defined in from to */
    public QualifiedName extract(int from, int to)
    {
        return new QualifiedName(catalog, parts.subList(from, to));
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof QualifiedName)
        {
            QualifiedName that = (QualifiedName) obj;
            return Objects.equals(catalog, that.catalog)
                && parts.equals(that.parts);

        }
        return false;
    }

    @Override
    public String toString()
    {
        return (catalog != null ? (catalog + "#") : "") + join(parts, ".");
    }

    public static QualifiedName of(String... parts)
    {
        return new QualifiedName(null, asList(parts));
    }
}
