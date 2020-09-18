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
package org.kuse.payloadbuilder.core.codegen;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.codehaus.janino.ClassBodyEvaluator;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Code generator */
public class CodeGenerator
{
    private static final String PREDICATE = System.lineSeparator() +
        "public boolean test(Object __row) " + System.lineSeparator() +
        "{ " + System.lineSeparator() +
        "  final Row row = (Row) __row;" + System.lineSeparator() +
        "  %s" + System.lineSeparator() +
        "  return !%s && %s;" + System.lineSeparator() +
        "}";

    private static final String FUNCTION = System.lineSeparator() +
        "public Object apply(Object __row) " + System.lineSeparator() +
        "{ " + System.lineSeparator() +
        "  final Row row = (Row) __row;" + System.lineSeparator() +
        "  %s" + System.lineSeparator() +
        "  return %s ? null : %s;" + System.lineSeparator() +
        "}";

    /** Generate a predicate. Used filters */
    public BasePredicate generatePredicate(TableAlias tableAlias, Expression expression)
    {
        ExpressionCode code = generate(expression, tableAlias);
        String generatedCode = String.format(PREDICATE, code.getCode(), code.getIsNull(), code.getResVar());
        return compile(code.getImports(), generatedCode, BasePredicate.class, "Predicate");
    }

    /** Generate a function. Used in expression projections etc. */
    public BaseFunction generateFunction(TableAlias tableAlias, Expression expression)
    {
        ExpressionCode code = generate(expression, tableAlias);
        String generatedCode = String.format(FUNCTION, code.getCode(), code.getIsNull(), code.getResVar());
        return compile(code.getImports(), generatedCode, BaseFunction.class, "Function");
    }

    /** Generate code for expression */
    private ExpressionCode generate(Expression expression, TableAlias tableAlias)
    {
        CodeGeneratorContext context = new CodeGeneratorContext();
        context.tableAlias = tableAlias;
        return expression.generateCode(context, null);
    }

    @SuppressWarnings("unchecked")
    private <T> T compile(Set<String> imports, String code, Class<T> baseClass, String name)
    {
        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setClassName("org.kuse.payloaduilder.codegen.Generated" + name);
        cbe.setExtendedClass(baseClass);

        List<String> usedImports = new ArrayList<>();
        usedImports.addAll(asList(
                "org.kuse.payloadbuilder.operator.Row",
                "org.kuse.payloadbuilder.parser.ExpressionMath"));
        usedImports.addAll(imports);

        cbe.setDefaultImports(usedImports.toArray(ArrayUtils.EMPTY_STRING_ARRAY));

        try
        {
            cbe.cook(code);
            Class<?> clazz = cbe.getClazz();
            return (T) clazz.getConstructors()[0].newInstance();
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Error compiling code: " + code, e);
        }

    }
}
