/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;

import java.util.List;

public class QualifiedName
{
    private final List<String> parts;

    public QualifiedName(List<String> parts)
    {
        this.parts = unmodifiableList(requireNonNull(parts));
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
        return new QualifiedName(parts.subList(from, to));
    }

    /** Extracts a new qualified name from this instance with parts defined in from to last part */
    public QualifiedName extract(int from)
    {
        if (from == 0)
        {
            return this;
        }
        return new QualifiedName(parts.subList(from, parts.size()));
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
            return parts.equals(that.parts);

        }
        return false;
    }

    @Override
    public String toString()
    {
        return join(parts, ".");
    }

    public static QualifiedName of(String... parts)
    {
        return new QualifiedName(asList(parts));
    }
}
