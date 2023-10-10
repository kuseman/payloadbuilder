package se.kuseman.payloadbuilder.api;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Qualified name */
public class QualifiedName
{
    public static QualifiedName EMPTY = new QualifiedName(emptyList());
    private final List<String> parts;

    public QualifiedName(List<String> parts)
    {
        this.parts = unmodifiableList(requireNonNull(parts));
    }

    public List<String> getParts()
    {
        return parts;
    }

    /** Returns size of this qualified name */
    public int size()
    {
        return parts.size();
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
        if (parts.size() > 0)
        {
            return parts.get(parts.size() - 1);
        }
        return "";
    }

    /** Get the first part in qualified name */
    public String getFirst()
    {
        if (parts.size() > 0)
        {
            return parts.get(0);
        }
        return "";
    }

    /** Extracts a new qualified name from this instance with parts defined in from to */
    public QualifiedName extract(int from, int to)
    {
        return new QualifiedName(parts.subList(from, to));
    }

    /** Extracts a new qualified name from this instance with parts defined in from to last part */
    public QualifiedName extract(int from)
    {
        if (from == 0)
        {
            return this;
        }
        else if (parts.size() == 1
                && from == 1)
        {
            return EMPTY;
        }
        else if (from > parts.size())
        {
            return EMPTY;
        }

        return new QualifiedName(parts.subList(from, parts.size()));
    }

    /** Extends this qualified name with provided part */
    public QualifiedName extend(String part)
    {
        List<String> parts = new ArrayList<>(this.parts.size() + 1);
        parts.addAll(this.parts);
        parts.add(part);
        return new QualifiedName(parts);
    }

    /** Prepend this qualified name with provided part */
    public QualifiedName prepend(String part)
    {
        List<String> parts = new ArrayList<>(this.parts.size() + 1);
        parts.add(part);
        parts.addAll(this.parts);
        return new QualifiedName(parts);
    }

    /** Returns a new qualified name with all parts lower cased */
    public QualifiedName toLowerCase()
    {
        List<String> parts = new ArrayList<>(this.parts.size());
        for (String part : this.parts)
        {
            parts.add(part.toLowerCase());
        }
        return new QualifiedName(parts);
    }

    /** Returns true if this qualified names parts equals other parts ignoring case */
    public boolean equalsIgnoreCase(QualifiedName name)
    {
        if (name == null)
        {
            return false;
        }

        int size = parts.size();
        if (size != name.size())
        {
            return false;
        }
        for (int i = 0; i < size; i++)
        {
            if (!parts.get(i)
                    .equalsIgnoreCase(name.getParts()
                            .get(i)))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof QualifiedName that)
        {
            int size = parts.size();
            if (size != that.parts.size())
            {
                return false;
            }
            for (int i = 0; i < size; i++)
            {
                if (!parts.get(i)
                        .equals(that.parts.get(i)))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString()
    {
        // Escape double quotes for each part
        // If part contains non char/digit/underscore quote part

        StringBuilder sb = new StringBuilder();
        for (String part : parts)
        {
            if (!sb.isEmpty())
            {
                sb.append('.');
            }

            int partStart = sb.length();
            boolean quote = false;
            int length = part.length();
            for (int i = 0; i < length; i++)
            {
                char c = part.charAt(i);
                if (c == '"')
                {
                    sb.append("\"");
                }

                sb.append(c);

                boolean isLetterOrUnderscore = Character.isLetter(c)
                        || c == '_';

                if ((i == 0
                        && !isLetterOrUnderscore)
                        || !(isLetterOrUnderscore
                                || Character.isDigit(c)))
                {
                    quote = true;
                }
            }

            if (quote)
            {
                sb.insert(partStart, "\"");
                sb.append("\"");
            }
        }

        return sb.toString();
    }

    /** Returns a dot delimited representation of this qualified name. NOTE! This won't quote needed parts etc. */
    public String toDotDelimited()
    {
        return parts.stream()
                .collect(joining("."));
    }

    /** Construct a qualified name from provided parts */
    public static QualifiedName of(String... parts)
    {
        if (parts.length == 0)
        {
            return EMPTY;
        }
        return new QualifiedName(asList(parts));
    }

    /**
     * Constuct a qualified name from provided object. If its a collection then parts is constructed from their string representations
     */
    public static QualifiedName of(Object object)
    {
        requireNonNull(object);
        if (object instanceof ArrayList)
        {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) object;
            int size = list.size();
            if (size == 1)
            {
                return new QualifiedName(singletonList(String.valueOf(list.get(0))));
            }
            List<String> parts = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                parts.add(String.valueOf(list.get(i)));
            }
            return new QualifiedName(parts);
        }
        else if (object instanceof Collection)
        {
            @SuppressWarnings("unchecked")
            Collection<Object> col = (Collection<Object>) object;
            return new QualifiedName(col.stream()
                    .map(Object::toString)
                    .collect(toList()));
        }
        return new QualifiedName(singletonList(String.valueOf(object)));
    }
}
