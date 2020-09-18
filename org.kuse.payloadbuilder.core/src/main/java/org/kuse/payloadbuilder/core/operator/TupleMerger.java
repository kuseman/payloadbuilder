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

/** Definition of a {@link Tuple} merger that merges two tuples. */
public interface TupleMerger
{
    /**
     * Merge outer and inner tuples.
     *
     * @param outer Outer tuple that is joined
     * @param inner Inner tuple that is joined
     * @param populating True if the join is populating. ie inner row is added to outer without creating a new tuple
     * @param nodeId Id of the operator node that this merge call belongs to.
     **/
    Tuple merge(Tuple outer, Tuple inner, boolean populating, int nodeId);
}
