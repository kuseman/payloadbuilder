package org.kuse.payloadbuilder.core.catalog;

import java.util.List;

/** Interface marking a function as a lambda function */
public interface LambdaFunction
{
    /**
     * Returns lambda bindings pairs. Left expression binds to right lambda expression
     *
     * <pre>
     * This is used to be able to correctly analyze expression return types to connect
     * which fields belongs to which aliases in a query.
     *
     * Ie.
     *
     * <i>map(list, x -> x.id)</i>
     * Here argument <b>list</b> binds to the lambda expression <b>x -> x.id</b>.
     * </pre>
     */
    List<LambdaBinding> getLambdaBindings();

    /** A lambda binding */
    class LambdaBinding
    {
        /** The index of the lambda argument */
        private final int lambdaArg;
        /** The index of the destination argument */
        private final int toArg;

        public LambdaBinding(int lambdaArg, int toArg)
        {
            this.lambdaArg = lambdaArg;
            this.toArg = toArg;
        }

        public int getLambdaArg()
        {
            return lambdaArg;
        }

        public int getToArg()
        {
            return toArg;
        }
    }
}
