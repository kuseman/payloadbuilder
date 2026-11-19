package se.kuseman.payloadbuilder.catalog.fs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Contents function, outputs path contents as string */
class ContentsFunction extends ScalarFunctionInfo
{
    ContentsFunction()
    {
        super("contents", FunctionType.SCALAR);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);
        final int size = input.getRowCount();
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Any);
            }

            @Override
            public int size()
            {
                return size;
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row);
            }

            @Override
            public Object getAny(int row)
            {
                String strPath = String.valueOf(value.getString(row)
                        .toString());
                try
                {
                    Path path = FileSystems.getDefault()
                            .getPath(strPath);
                    return new FSInputStream(path.toFile());
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Error reading contents from: " + strPath, e);
                }
            }
        };
    }

    static class FSInputStream extends BufferedInputStream
    {
        private final File file;

        public FSInputStream(File file) throws FileNotFoundException
        {
            super(new FileInputStream(file));
            this.file = file;
        }

        @Override
        public String toString()
        {
            // Fallback to read the file to string for operators and functions not supporting a reader
            try
            {
                return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error reading file contents to string", e);
            }
        }
    }
}
