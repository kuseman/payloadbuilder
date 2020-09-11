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

/** Definition of a query result */
public interface QueryResult
{
    /** Returns true if there are more result sets */
    boolean hasMoreResults();

    /**
     * Write current result set to provided writer
     *
     * @throws IllegalArgumentException if there are no more results
     */
    void writeResult(OutputWriter writer);

    /** Resets the query result to initial state */
    void reset();
}
