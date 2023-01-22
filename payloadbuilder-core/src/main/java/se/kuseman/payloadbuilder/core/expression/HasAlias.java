package se.kuseman.payloadbuilder.core.expression;

/** Marker for expressions that has a name. Ie. alias expression column expressions etc. */
public interface HasAlias
{
    /** Return the alias of this expression */
    Alias getAlias();

    /** Alias */
    static class Alias
    {
        public static Alias EMPTY = new Alias("", "");

        private final String alias;
        private final String outputAlias;

        public Alias(String alias, String outputAlias)
        {
            this.alias = alias;
            this.outputAlias = outputAlias;
        }

        public String getAlias()
        {
            return alias;
        }

        public String getOutputAlias()
        {
            return outputAlias;
        }
    }
}
