package se.kuseman.payloadbuilder.catalog.es;

import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMeta.Version;

/** Test with elastic search 6X */
class ES6XTest extends BaseESTest
{
    private static final String TYPE = "testType";

    ES6XTest()
    {
        super(ES.ES_ENDPOINT, TYPE, Version._6X);
    }

    @AfterAll
    static void tearDownClass()
    {
        ES.stop();
    }

    static class ES
    {
        private static final int PORT = 9200;
        private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:6.8.23";
        @SuppressWarnings("resource")
        private static final GenericContainer<?> ES_CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME)).withExposedPorts(PORT)
                .withEnv(Map.of("xpack.security.enabled", "false", "discovery.type", "single-node"));

        static final String ES_ENDPOINT;

        static
        {
            ES_CONTAINER.setWaitStrategy(new HostPortWaitStrategy());
            ES_CONTAINER.withEnv("xpack.security.enabled", "false")
                    .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=536870912");
            ES_CONTAINER.start();

            ES_ENDPOINT = "http://" + ES_CONTAINER.getHost() + ":" + ES_CONTAINER.getMappedPort(PORT);

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
