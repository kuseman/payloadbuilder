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

/** Visitor definition of tree */
public interface SelectVisitor<TR, TC>
{
    TR visit(Select select, TC context);

    TR visit(TableSourceJoined joinedTableSource, TC context);

    TR visit(SortItem sortItem, TC context);

    TR visit(ExpressionSelectItem expressionSelectItem, TC context);

    TR visit(NestedSelectItem nestedSelectItem, TC context);

    TR visit(AsteriskSelectItem selectItem, TC context);

    TR visit(Table table, TC context);

    TR visit(TableFunction tableFunction, TC context);

    TR visit(Join join, TC context);

    TR visit(Apply apply, TC context);

    TR visit(SubQueryTableSource populatingJoin, TC context);
}
