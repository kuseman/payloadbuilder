package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;

/** A location in query text */
public record Location(int line, int startOffset, int endOffset, String text)
{

    public static Location EMPTY = new Location(0, 0, 0, "");

    public Location
    {
        requireNonNull(text);
    }

    /** Create a location from provided rule context */
    public static Location from(ParserRuleContext ctx)
    {
        return new Location(ctx.start.getLine(), ctx.start.getStartIndex(), ctx.stop.getStopIndex() + 1, "");
    }

    /** Create a location from provided rule context */
    public static Location from(TerminalNode node)
    {
        return new Location(node.getSymbol()
                .getLine(),
                node.getSymbol()
                        .getStartIndex(),
                node.getSymbol()
                        .getStopIndex() + 1,
                "");
    }

    /** Create a location from provided rule context extracting the rule text. */
    public static Location withText(ParserRuleContext ctx)
    {
        String text = ctx.start.getInputStream()
                .getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));

        return new Location(ctx.start.getLine(), ctx.start.getStartIndex(), ctx.stop.getStopIndex() + 1, text);
    }

}
