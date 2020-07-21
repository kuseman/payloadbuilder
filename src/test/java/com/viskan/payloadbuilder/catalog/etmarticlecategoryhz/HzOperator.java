package com.viskan.payloadbuilder.catalog.etmarticlecategoryhz;

import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.AOperator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.OperatorContext.NodeData;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.ExecutionContext;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Collections.emptyIterator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

class HzOperator extends AOperator
{
    private static final ObjectMapper SMILE_MAPPER = new ObjectMapper(new SmileFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final HazelcastInstance hazelcast;
    private final TableAlias tableAlias;
    private final Index index;

    public HzOperator(HazelcastInstance hazelcast, int nodeId, TableAlias tableAlias)
    {
        this(hazelcast, nodeId, tableAlias, null);
    }
    
    public HzOperator(HazelcastInstance hazelcast, int nodeId, TableAlias tableAlias, Index index)
    {
        super(nodeId);
        this.hazelcast = hazelcast;
        this.tableAlias = tableAlias;
        this.index = index;
    }

    /* SETTINGS FIELDS */

    private final Map<String, String> TABLE_TO_MAP = ofEntries(
            entry("article", "EtmArticle"),
            entry("articleName", "EtmArticleName"),
            entry("articleBalance", "EtmArticleBalance"),
            entry("articleAttribute", "EtmArticleAttribute"),
            entry("articlePrice", "EtmArticlePrice"),
            entry("articleCategory", "EtmArticleCategory"),
            entry("articleProperty", "EtmArticleProperty"),
            entry("articleAttributeMedia", "EtmArticleAttributeMedia"),
            entry("propertyKey", "EtmPropertyKey"),
            entry("propertyValue", "EtmPropertyValue"),
            entry("attribute1", "EtmAttribute1"),
            entry("attribute2", "EtmAttribute2"),
            entry("attribute3", "EtmAttribute3"),
            entry("stockhouse", "EtmStockhouse"),
            entry("mediaName", "EtmMediaName"),
            entry("category", "EtmCategory"),
            entry("hierarchy", "EtmHierarchy"),
            entry("contentCategoryHead", "EtmContentCategoryHead")
            
            );

    /* END SETTINGS FIELD */

    /* OPERATOR CONTEXT FIELDS */

    //        String database = "RamosLager157TestDev".toLowerCase();
    //    String database = "RamosOscarJacobsonTestCust".toLowerCase();
    String database = "RamosSportshopenTestCust".toLowerCase();
//        String database = "RamosVnpTestMain".toLowerCase();
    int comp_id = 0;
    String instance = database + (comp_id > 0 ? "_" + comp_id : "");
    String mapPrefix = instance + "_";

    /* END OPERATOR CONTEXT FIELDS */

    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        Data data = getData(context.getOperatorContext());
        Map<Object, byte[]> map = data.mapRows;
        // Index operator, extract keys and retrieve map of those keys
        if (index != null)
        {
            Set<Object> keys = new HashSet<>();
            while (context.getOperatorContext().getOuterIndexValues().hasNext())
            {
                Object[] array = context.getOperatorContext().getOuterIndexValues().next();
                // TODO: extract correct keys
                Object key = array[0];
                if (key != null)
                {
                    keys.add(key);
                }
            }

            if (keys.isEmpty())
            {
                return emptyIterator();
            }
            
            map = data.mapRows.getAll(keys);
        }

        return getIterator(data.columnIndices, map);
    }

    private Iterator<Row> getIterator(
            int[] columnIndices,
            Map<Object, byte[]> map)
    {
        final Iterator<byte[]> it = map.values().iterator();
        return new Iterator<Row>()
        {
            int pos = 0;
            Row next;
            MapRows current;
            int rowsIndex;

            @Override
            public Row next()
            {
                Row r = next;
                next = null;
                return r;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (current == null)
                    {
                        if (!it.hasNext())
                        {
                            return false;
                        }

                        current = get(it.next(), MapRows.class);
                        continue;
                    }
                    else if (rowsIndex >= current.getRows().size())
                    {
                        current = null;
                        rowsIndex = 0;
                        continue;
                    }

                    Object[] row = current.getRows().get(rowsIndex);
                    next = Row.of(tableAlias, pos++, new MapRowsValues(row, columnIndices));
                    rowsIndex++;
                }

                return true;
            }
        };
    }

    private Data getData(OperatorContext context)
    {
        String table = tableAlias.getTable().toString();
        return context.getNodeData(nodeId, () ->
        {
            String mapName = TABLE_TO_MAP.get(table);
            if (mapName == null)
            {
                throw new IllegalArgumentException("Table " + table + " not configued");
            }

            String fullMapName = mapPrefix + mapName;
            String fullMapMetaName = mapPrefix + "MapMeta";
            Map<String, byte[]> mapMetaByMap = hazelcast.getMap(fullMapMetaName);
            byte[] bytes = mapMetaByMap.get(mapName);
            MapMeta mapMeta = get(bytes, MapMeta.class);
            IMap<Object, byte[]> mapRows = hazelcast.getMap(fullMapName);
            
            int length = tableAlias.getColumns().length;
            int[] columnIndices = new int[length];
            for (int i = 0; i < length; i++)
            {
                int index = ArrayUtils.indexOf(mapMeta.getColumns(), tableAlias.getColumns()[i]);
                columnIndices[i] = index;
            }

            Data d = new Data();
            d.mapRows = mapRows;
            d.columnIndices = columnIndices;
            return d;
        });
    }

    private <V> V get(byte[] bytes, Class<V> clazz)
    {
        try
        {
            return SMILE_MAPPER.readValue(bytes, clazz);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error", e);
        }
    }

    @Override
    public String toString()
    {
        String format = "SCAN(ID: %d, TABLE: %s)";
        if (index != null)
        {
            format = "INDEX SCAN(ID: %d, TABLE: %s, INDEX: %s)";
        }
        return String.format(format, nodeId, tableAlias.getTable(), index != null ? index.getColumns() : null);
    }

    /** Container for HZ data to reduce tension on HZ communication */
    private static class Data extends NodeData
    {
        IMap<Object, byte[]> mapRows;
        int[] columnIndices;
    }

    /**
     * Values implementation that uses original map rows array and translates ordinals upon get
     */
    private static class MapRowsValues implements Row.Values
    {
        private final Object[] row;
        private final int[] columnIndices;

        MapRowsValues(Object[] row, int[] columnIndices)
        {
            this.row = row;
            this.columnIndices = columnIndices;
        }

        @Override
        public Object get(int ordinal)
        {
            int index = columnIndices[ordinal];
            return index != -1 ? row[index] : null;
        }
    }
}
