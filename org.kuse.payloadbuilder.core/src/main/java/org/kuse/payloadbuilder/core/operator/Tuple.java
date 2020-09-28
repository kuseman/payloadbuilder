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
