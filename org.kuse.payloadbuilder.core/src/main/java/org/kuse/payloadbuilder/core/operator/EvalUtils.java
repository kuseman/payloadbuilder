package org.kuse.payloadbuilder.core.operator;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.IOUtils;

/** Utils for evaluation of expressions */
public final class EvalUtils
{
    private EvalUtils()
    {
    }

    /**
     * Unwraps provided objects.
     *
     * <pre>
     * There are special types provided by functions in the pipe when evaluating expression
     * and these objects need to be processed to make something useful of them.
     * These are:
     *   - Iterator (make a list of it)
     *   - Reader (Read to string)
     * </pre>
     */
    @SuppressWarnings("unchecked")
    public static Object unwrap(Object object)
    {
        if (object instanceof Iterator)
        {
            return IteratorUtils.toList((Iterator<Object>) object);
        }
        else if (object instanceof Reader)
        {
            try
            {
                return IOUtils.toString((Reader) object);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error trying to read Reader", e);
            }
        }

        return object;
    }
}
