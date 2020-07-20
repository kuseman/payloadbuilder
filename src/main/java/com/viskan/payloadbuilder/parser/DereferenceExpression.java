package com.viskan.payloadbuilder.parser;

import static java.util.Objects.requireNonNull;

public class DereferenceExpression extends Expression
{
    private final Expression left;
    private final QualifiedReferenceExpression right;

    public DereferenceExpression(Expression left, QualifiedReferenceExpression right)
    {
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
    }

    public Expression getLeft()
    {
        return left;
    }

    public QualifiedReferenceExpression getRight()
    {
        return right;
    }

//    @Override
//    public Object eval(ExecutionContext context)
//    {
//        Object leftResult = left.eval(context);
//
//        /** Extract first result from iterator */
//        if (leftResult instanceof Iterator)
//        {
//            @SuppressWarnings("unchecked")
//            Iterator<Object> it = (Iterator<Object>) leftResult;
//            leftResult = it.hasNext() ? it.next() : null;
//        }
//        if (leftResult == null)
//        {
//            return null;
//        }
//
//        throw new RuntimeException("IMPLEMENT");
//        
////        QualifiedReferenceContainer container = evaluationContext.getContainer(right.getQname(), right.getUniqueId());
////        return container.getValue(leftResult);
//    }
//
//    @Override
//    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
//    {
//        /*
//         * a.func().b
//         *
//         * Input res_1 = new Input(a.func());
//         * res_1 = res_1.getObject('b');
//         *
//         *
//         */
//
//        System.out.println(right);
//
//        ExpressionCode leftCode = left.generateCode(context, parentCode);
//        ExpressionCode code = ExpressionCode.code(context, leftCode);
//
//        String template = "%s"
//            + "boolean %s = %s != null;\n"
//            + "Object %s = null;\n"
//            + "if (!%s)\n"
//            + "{\n"
//            + "  %s = dereference(%s, %s);\n"
//            + "  %s = %s != null;\n"
//            + "}\n";
//
//        code.setCode(String.format(template,
//                leftCode.getCode(),
//                code.getIsNull(), leftCode.getResVar(),
//                code.getResVar(),
//                leftCode.getIsNull(),
//                code.getResVar(), leftCode.getResVar(), "\"rightCode\"",
//                code.getIsNull(), code.getResVar()));
//
//        return code;
//    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return left.isNullable() && left.isNullable();
    }

    @Override
    public int hashCode()
    {
        return 17
            + (37 * left.hashCode())
            + (37 * right.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof DereferenceExpression)
        {
            DereferenceExpression that = (DereferenceExpression) obj;
            return left.equals(that.left)
                && right.equals(that.right);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return left.toString() + "." + right.toString();
    }
}
