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

import java.util.Iterator;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Result produced by an {@link Operator} */
public interface Tuple
{
    /** Returns true if this tuple contains provided alias */
    boolean containsAlias(String alias);

    /**
     * Resolve value from provided qualified name
     *
     * @param qname Qualified name to resolve
     * @param partIndex From which index to start resolve in {@link QualifiedName#getParts()}
     */
    Object getValue(QualifiedName qname, int partIndex);

    /** Returns an iterator of qualified names for this tuple. Used to resolve q-names when using asterisk selects. */
    Iterator<QualifiedName> getQualifiedNames();
}
