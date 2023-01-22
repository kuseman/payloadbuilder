package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMetaUtils.MappedType;

/** Meta about an elasticsearch instance such as version etc. */
class ElasticsearchMeta
{
    private final Version version;
    private final Map<String, MappedType> mappedTypes;

    ElasticsearchMeta(Version version, Map<String, MappedType> mappedTypes)
    {
        this.version = requireNonNull(version, "version");
        this.mappedTypes = requireNonNull(mappedTypes, "mappedTypes");
    }

    Version getVersion()
    {
        return version;
    }

    ElasticStrategy getStrategy()
    {
        return version.strategy;
    }

    Map<String, MappedType> getMappedTypes()
    {
        return mappedTypes;
    }

    enum Version
    {
        _1X(new Elastic1XStrategy()),
        _2X(new Elastic2XStrategy()),
        _5X(new Elastic5XStrategy()),
        _6X(new Elastic6XStrategy()),
        _7X(new Elastic7XStrategy()),
        _8X(new Elastic8XStrategy()),
        _XX(new Elastic8XStrategy());

        private final ElasticStrategy strategy;

        private Version(ElasticStrategy strategy)
        {
            this.strategy = strategy;
        }

        ElasticStrategy getStrategy()
        {
            return strategy;
        }

        static Version fromString(String version)
        {
            String[] parts = version.split("\\.");
            if ("1".equals(parts[0]))
            {
                return _1X;
            }
            else if ("2".equals(parts[0]))
            {
                return _2X;
            }
            else if ("5".equals(parts[0]))
            {
                return _5X;
            }
            else if ("6".equals(parts[0]))
            {
                return _6X;
            }
            else if ("7".equals(parts[0]))
            {
                return _7X;
            }
            else if ("8".equals(parts[0]))
            {
                return _8X;
            }
            else
            {
                return _XX;
            }
        }
    }
}
