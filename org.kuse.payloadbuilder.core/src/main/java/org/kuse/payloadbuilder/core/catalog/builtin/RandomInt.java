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
package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.List;
import java.util.Random;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Function random int, returns a random int from provided seed. */
class RandomInt extends ScalarFunctionInfo
{
    private final Random random;

    RandomInt(Catalog catalog)
    {
        super(catalog, "randomInt");
        this.random = new Random();
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object boundObj = arguments.get(0).eval(context);
        if (boundObj == null)
        {
            return null;
        }
        int bound = ((Number) boundObj).intValue();
        return random.nextInt(bound);
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode, List<Expression> arguments)
    {
        ExpressionCode code = ExpressionCode.code(context);
        code.addImport("java.util.Random");

        ExpressionCode argCode = arguments.get(0).generateCode(context, parentCode);

        code.setCode(String.format(
                "%s"
                    + "boolean %s = true;\n"
                    + "int %s = 0;\n"
                    + "if (!%s)\n"
                    + "{\n"
                    + "  %s = false;\n"
                    + "  %s = new java.util.Random().nextInt(((Number) %s).intValue());\n"
                    + "}\n",
                argCode.getCode(),
                code.getIsNull(),
                code.getResVar(),
                argCode.getIsNull(),
                code.getIsNull(),
                code.getResVar(),
                argCode.getResVar()));
        return code;
    }
}
