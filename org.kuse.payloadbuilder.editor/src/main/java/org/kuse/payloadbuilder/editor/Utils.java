package org.kuse.payloadbuilder.editor;

/** Variuos utils used by Editor */
final class Utils
{
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

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
}
