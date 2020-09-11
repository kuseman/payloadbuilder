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
package org.kuse.payloadbuilder.core.operator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;

import org.kuse.payloadbuilder.core.OutputWriter;

/** Writer that writes output as JSON string */
public class JsonStringWriter implements OutputWriter
{
    private StringBuilder sb = new StringBuilder();

    public void append(String string)
    {
        sb.append(string);
    }

    @Override
    public void endRow()
    {
        sb.append(System.lineSeparator());
    }

    public String getAndReset()
    {
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ',')
        {
            sb.deleteCharAt(sb.length() - 1);
        }
        String result = sb.toString();
        sb = new StringBuilder();
        return result;
    }

    @Override
    public void writeFieldName(String name)
    {
        sb.append("\"").append(name).append("\":");
    }

    @Override
    public void writeValue(Object input)
    {
        Object value = input;
        if (value instanceof Iterator)
        {
            @SuppressWarnings("unchecked")
            Iterator<Object> it = (Iterator<Object>) value;
            startArray();
            while (it.hasNext())
            {
                writeValue(it.next());
            }
            endArray();
            return;
        }

        if (value instanceof String)
        {
            sb.append("\"");
        }
        if (value instanceof Float)
        {
            value = new BigDecimal((Float) value).setScale(2, RoundingMode.HALF_UP);
        }
        else if (value instanceof Double)
        {
            value = new BigDecimal((Double) value).setScale(2, RoundingMode.HALF_UP);
        }
        sb.append(value);
        if (value instanceof String)
        {
            sb.append("\"");
        }

        sb.append(",");
    }

    @Override
    public void startObject()
    {
        sb.append("{");
    }

    @Override
    public void endObject()
    {
        if (sb.charAt(sb.length() - 1) != '{')
        {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("}");
        sb.append(",");
    }

    @Override
    public void startArray()
    {
        sb.append("[");
    }

    @Override
    public void endArray()
    {
        if (sb.charAt(sb.length() - 1) != '[')
        {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("]");
        sb.append(",");
    }
}
