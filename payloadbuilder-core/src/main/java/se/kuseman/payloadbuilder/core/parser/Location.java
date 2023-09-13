package se.kuseman.payloadbuilder.core.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

/** A location in query text */
public record Location(int line, int startOffset, int endOffset)
{

    public static Location EMPTY = new Location(0, 0, 0);

    /** Create a location from provided rule context */
    public static Location from(ParserRuleContext ctx)
    {
        return new Location(ctx.start.getLine(), ctx.start.getStartIndex(), ctx.stop.getStopIndex() + 1);
    }

    /** Create a location from provided rule context */
    public static Location from(TerminalNode node)
    {
        return new Location(node.getSymbol()
                .getLine(),
                node.getSymbol()
                        .getStartIndex(),
                node.getSymbol()
                        .getStopIndex() + 1);
    }
}
