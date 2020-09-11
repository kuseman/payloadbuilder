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

/** Visitor definition of expressions */
public interface ExpressionVisitor<TR, TC>
{
    TR visit(LiteralNullExpression expression, TC context);

    TR visit(LiteralBooleanExpression expression, TC context);

    TR visit(LiteralIntegerExpression expression, TC context);

    TR visit(LiteralLongExpression expression, TC context);

    TR visit(LiteralFloatExpression expression, TC context);

    TR visit(LiteralDoubleExpression expression, TC context);

    TR visit(LiteralStringExpression expression, TC context);

    TR visit(ComparisonExpression expression, TC context);

    TR visit(ArithmeticUnaryExpression expression, TC context);

    TR visit(ArithmeticBinaryExpression expression, TC context);

    TR visit(LogicalBinaryExpression expression, TC context);

    TR visit(LogicalNotExpression expression, TC context);

    TR visit(InExpression expression, TC context);

    TR visit(QualifiedReferenceExpression expression, TC context);

    TR visit(NestedExpression expression, TC context);

    TR visit(NullPredicateExpression expression, TC context);

    TR visit(QualifiedFunctionCallExpression expression, TC context);

    TR visit(DereferenceExpression expression, TC context);

    TR visit(LambdaExpression expression, TC context);

    TR visit(VariableExpression expression, TC context);

    TR visit(SubscriptExpression expression, TC context);
}
