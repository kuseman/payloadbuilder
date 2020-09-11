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

/** Visitor definition of statements */
public interface StatementVisitor<TR, TC>
{
    /* Control flow nodes */
    TR visit(IfStatement statement, TC context);

    TR visit(PrintStatement statement, TC context);

    /* Misc nodes */
    TR visit(SetStatement statement, TC context);

    TR visit(UseStatement statement, TC context);

    TR visit(DescribeTableStatement statement, TC context);

    TR visit(DescribeSelectStatement statement, TC context);

    TR visit(ShowStatement statement, TC context);

    /* DML nodes */
    TR visit(SelectStatement statement, TC context);
}
