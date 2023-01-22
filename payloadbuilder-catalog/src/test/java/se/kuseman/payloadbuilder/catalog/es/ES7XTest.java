package se.kuseman.payloadbuilder.catalog.es;

import java.util.Map;

import org.junit.AfterClass;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMeta.Version;

/** Test with elastic search 7X */
public class ES7XTest extends BaseESTest
{
    public ES7XTest()
    {
        super(ES.ES_ENDPOINT, ESCatalog.SINGLE_TYPE_TABLE_NAME, Version._7X);
    }

    @AfterClass
    public static void tearDownClass()
    {
        ES.stop();
    }

    @Override
    protected Map<String, Object> modifyMappingProperties(Map<String, Object> mappings)
    {
        // indices created with ES 7.X doesn't have any types so return mappings plainly
        return mappings;
    }

    static class ES
    {
        private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:7.14.0";
        private static final ElasticsearchContainer ES_CONTAINER = new ElasticsearchContainer(IMAGE_NAME);

        static final String ES_ENDPOINT;

        static
        {
            ES_CONTAINER.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*\"message\":.*\"started\".*"));
            ES_CONTAINER.withEnv("xpack.security.enabled", "false")
                    .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=536870912");
            ES_CONTAINER.start();

            ES_ENDPOINT = "http://" + ES_CONTAINER.getHttpHostAddress();

            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> ES_CONTAINER.stop()));
        }

        static void stop()
        {
            if (ES_CONTAINER != null)
            {
                ES_CONTAINER.stop();
            }
        }
    }
}
