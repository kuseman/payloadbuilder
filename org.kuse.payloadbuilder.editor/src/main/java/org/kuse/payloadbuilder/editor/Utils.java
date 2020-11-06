package org.kuse.payloadbuilder.editor;

import java.io.StringReader;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Node;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;

/** Variuos utils used by Editor */
final class Utils
{
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    private static final DOMImplementationLS DOM;

    static
    {
        try
        {
            DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
            DOM = (DOMImplementationLS) registry.getDOMImplementation("LS");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error creating XML factory");
        }
    }

    /** Get HEX represenatation of a byte arrya */
    static String bytesToHex(byte[] bytes)
    {
        //CSOFF
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++)
        {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        //CSON
        return "0x" + new String(hexChars);
    }

    /** Format provided xml */
    static String formatXML(String xml)
    {
        try
        {
            final InputSource src = new InputSource(new StringReader(xml));
            final Node document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(src).getDocumentElement();
            final Boolean keepDeclaration = Boolean.valueOf(xml.startsWith("<?xml"));

            final LSSerializer writer = DOM.createLSSerializer();

            writer.getDomConfig().setParameter("format-pretty-print", Boolean.TRUE);
            writer.getDomConfig().setParameter("xml-declaration", keepDeclaration);

            return writer.writeToString(document);
        }
        catch (Exception e)
        {
            // Return original upon error
            return xml;
        }
    }
}
