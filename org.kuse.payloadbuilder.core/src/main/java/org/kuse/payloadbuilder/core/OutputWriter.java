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
package org.kuse.payloadbuilder.core;

/** Output writer that writes generated output */
public interface OutputWriter
{
    /**
     * Start a new a result set with provided columns.
     *
     * @param columns Columns for this result result. NOTE! Can be null if columns are unknown (select *).
     */
    default void initResult(String[] columns)
    {
    }

    /** Start a new row. Called each time before a new row is to be written. */
    default void startRow()
    {
    }

    /** End row. Called when current row is complete */
    default void endRow()
    {
    }

    /** Write field name */
    void writeFieldName(String name);

    /** Write value */
    void writeValue(Object value);

    /** Start object */
    void startObject();

    /** End object */
    void endObject();

    /** Start array */
    void startArray();

    /** End array */
    void endArray();
}
