package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.catalog.es.ESDatasource.MAPPER;
import static se.kuseman.payloadbuilder.test.ExpressionTestUtils.ce;
import static se.kuseman.payloadbuilder.test.ExpressionTestUtils.col;
import static se.kuseman.payloadbuilder.test.ExpressionTestUtils.createNullExpression;
import static se.kuseman.payloadbuilder.test.ExpressionTestUtils.createStringExpression;
import static se.kuseman.payloadbuilder.test.ExpressionTestUtils.not;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMetaUtils.MappedProperty;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.parser.QueryParser;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;
import se.kuseman.payloadbuilder.test.IPredicateMock;

/** Test of {@link ElasticQueryBuilder}. */
public class ElasticQueryBuilderTest
{
    private static final QueryParser PARSER = new QueryParser();
    private QuerySession session;
    private ExecutionContext context;

    @Before
    public void setup()
    {
        session = new QuerySession(new CatalogRegistry());
        context = new ExecutionContext(session);
    }

    @Test
    public void test_single_property_is_identified() throws JsonMappingException, JsonProcessingException
    {
        //@formatter:off
        List<IPredicate> predicates = new ArrayList<>(List.of(
                IPredicateMock.undefined(e("(__index = 'stage')"))
        ));
        //@formatter:on

        List<IPropertyPredicate> result = ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", emptyMap());
        assertTrue(predicates.isEmpty());
        assertEquals("_index", result.get(0)
                .getProperty());
        assertEquals("__index = 'stage'", result.get(0)
                .getDescription());

        //@formatter:off
        predicates = new ArrayList<>(List.of(
                IPredicateMock.undefined(e("(__index = 'stage' AND __id = 'id')"))
        ));
        //@formatter:on

        result = ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", emptyMap());
        assertTrue(predicates.isEmpty());
        assertNull(result.get(0)
                .getProperty());
        assertEquals("__index = 'stage' AND __id = 'id'", result.get(0)
                .getDescription());

    }

    @Test
    public void test_collect_1x_2x_strategy() throws JsonMappingException, JsonProcessingException
    {
        // Verify special constructs like existence of filter in bool clause etc.
        //@formatter:off
        List<IPredicate> predicates = new ArrayList<>(List.of(
                IPredicateMock.undefined(e("(__index = 'app1' AND __id = 'stage')")),
                IPredicateMock.undefined(e("(__index = null)")),
                IPredicateMock.undefined(e("__index = 'test' OR NOT (__id = null)")),
                IPredicateMock.undefined(ExpressionTestUtils.and(
                        ce(IComparisonExpression.Type.EQUAL, col("__id"), createStringExpression("hello")),
                        not(ce(IComparisonExpression.Type.EQUAL, col("__index"), createNullExpression()))))
        ));
        //@formatter:on
        List<IPropertyPredicate> predicatesResult = ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", emptyMap());
        // All predicates should be consumed
        assertTrue(predicates.isEmpty());

        StringBuilder filter = new StringBuilder();
        StringBuilder filterNot = new StringBuilder();

        for (IPropertyPredicate pp : predicatesResult)
        {
            int l1 = filter.length();
            int l2 = filterNot.length();
            pp.appendBooleanClause(false, ElasticsearchMeta.Version._1X.getStrategy(), filter, filterNot, context);
            if (l1 != filter.length())
            {
                filter.append(',');
            }
            if (l2 != filterNot.length())
            {
                filterNot.append(',');
            }
        }
        if (filter.charAt(filter.length() - 1) == ',')
        {
            filter.deleteCharAt(filter.length() - 1);
        }
        if (filterNot.length() > 0
                && filterNot.charAt(filterNot.length() - 1) == ',')
        {
            filterNot.deleteCharAt(filterNot.length() - 1);
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> filterActual = ESDatasource.MAPPER.readValue("[" + filter + "]", List.class);
        assertEquals(4, filterActual.size());
        assertPredicate("""
                {
                  "bool" : {
                    "must" : [ {
                      "term" : {
                        "_index" : "app1"
                      }
                    }, {
                      "term" : {
                        "_id" : "stage"
                      }
                    } ]
                  }
                }
                """, filterActual.get(0));
        assertPredicate("""
                {
                  "term" : {
                    "_id" : "---1"
                  }
                }
                """, filterActual.get(1));
        assertPredicate("""
                  {
                  "bool" : {
                    "should" : [ {
                      "term" : {
                        "_index" : "test"
                      }
                    }, {
                      "term" : {
                        "_id" : "---1"
                      }
                    } ]
                  }
                }
                  """, filterActual.get(2));
        assertPredicate("""
                  {
                  "bool" : {
                    "must" : [ {
                      "term" : {
                        "_id" : "hello"
                      }
                    }, {
                      "bool" : {
                        "must_not" : [ {
                          "match_all" : { }
                        } ]
                      }
                    } ]
                  }
                }
                  """, filterActual.get(3));
    }

    @Test
    public void test_collect_not_supported()
    {
        Map<QualifiedName, MappedProperty> properties = emptyMap();
        List<IPredicate> predicates;

        // <col> = <col1> is not suppored
        predicates = List.of(IPredicateMock.undefined(e("__index = __id")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // Comparisons must have qualifiers on one of the sides
        predicates = List.of(IPredicateMock.undefined(e("(__index > 10) = (__index > 20)")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // Built in columns only supports equal/not equal
        predicates = List.of(IPredicateMock.undefined(e("__index > 'some_index'")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // Not a mapped property
        predicates = List.of(IPredicateMock.undefined(e("nono > 'some_index'")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        properties = Map.of(QualifiedName.of("application"), MappedProperty.of(QualifiedName.of("application"), "keyword"));
        // Cannot do a range query on string's
        predicates = List.of(IPredicateMock.undefined(e("application > 'some_index'")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // Freetext mapping with no non freetext field
        properties = Map.of(QualifiedName.of("message"), MappedProperty.of(QualifiedName.of("message"), "text"));
        predicates = List.of(IPredicateMock.undefined(e("message = 'some_index'")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // Like on a non mapped field
        properties = emptyMap();
        predicates = List.of(IPredicateMock.undefined(e("message like '%some_index'")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // In expression with no mapped columns
        properties = emptyMap();
        predicates = List.of(IPredicateMock.undefined(e("message in ('value')")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());
        properties = emptyMap();
        predicates = List.of(IPredicateMock.undefined(e("'value' in (message)")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // No non-free-text-mapping
        properties = Map.of(QualifiedName.of("message"), MappedProperty.of(QualifiedName.of("message"), "text"));
        predicates = List.of(IPredicateMock.undefined(e("message in ('value')")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // Not a supported function
        properties = emptyMap();
        predicates = List.of(IPredicateMock.function("sys", "info", List.of(e("msg"))));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());
        predicates = List.of(IPredicateMock.function("sys", "bool_func", List.of()));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // Unsupported expression
        predicates = List.of(IPredicateMock.undefined(e("__index + 1 > 0")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());
        predicates = List.of(IPredicateMock.undefined(e("__index = 'test' or __id +1 = 0")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // Test turning off like expression
        session.setCatalogProperty("es", ESCatalog.USE_LIKE_EXPRESSION, false);
        predicates = List.of(IPredicateMock.undefined(e("__index LIKE '%valie'")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());
        session.setCatalogProperty("es", ESCatalog.USE_LIKE_EXPRESSION, true);

        // Template string with column
        predicates = List.of(IPredicateMock.undefined(e("__index = `text + ${__id}`")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // No qualifier for in
        predicates = List.of(IPredicateMock.undefined(e("(__index = 'test') in (true)")));
        ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        assertEquals(1, predicates.size());

        // // Make sure we don't a function that is returning a bool. ie. a single function
        // predicates = List.of(IPredicateMock.undefined(e("bool_func()")));
        // ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        // assertEquals(1, predicates.size());
        // predicates = List.of(IPredicateMock.undefined(e("bool_func(__index)")));
        // ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);
        // assertEquals(1, predicates.size());
    }

    @Test
    public void test_collect() throws JsonMappingException, JsonProcessingException
    {
        //@formatter:off
        Map<QualifiedName, MappedProperty> properties = Map.of(
                QualifiedName.of("application"), MappedProperty.of(QualifiedName.of("application"), "keyword"),
                QualifiedName.of("deploy"), MappedProperty.of(QualifiedName.of("deploy"), "keyword"), 
                QualifiedName.of("http", "request", "id"), MappedProperty.of(QualifiedName.of("http", "request", "id"), "keyword"), 
                QualifiedName.of("trace", "id"), MappedProperty.of(QualifiedName.of("trace", "id"), "keyword"), 
                QualifiedName.of("level"), MappedProperty.of(QualifiedName.of("level"), "keyword"),
                QualifiedName.of("nest", "field"), MappedProperty.of(QualifiedName.of("nest", "field"), "integer", QualifiedName.of("nest")),
                QualifiedName.of("msg"), new MappedProperty(QualifiedName.of("msg"), "text", null, null, List.of(
                        MappedProperty.of(QualifiedName.of("msg", "keyword"), "keyword")
                        ), emptyMap()));
        List<IPredicate> predicates = new ArrayList<>(List.of(
                IPredicateMock.undefined(e("NOT (application = 'app1' AND deploy = 'stage')")),
                IPredicateMock.undefined(e("NOT (application = 'app2' AND deploy = 'prod')")),
                IPredicateMock.undefined(e("application = 'kappahl'")),
                IPredicateMock.undefined(e("deploy IN (1,2,3)")),
                IPredicateMock.undefined(e("msg NOT IN ('hello',2,3)")),
                IPredicateMock.undefined(e("'traceid' IN (http.request.id, trace.id, msg)")),
                IPredicateMock.undefined(e("'' + null IN (http.request.id, trace.id, msg)")),
                IPredicateMock.undefined(e("'traceid' not IN (deploy, application)")),
                IPredicateMock.undefined(e("'index1' = __index OR __id = null")),
                IPredicateMock.undefined(e("__index LIKE '%tenant%'")),
                IPredicateMock.undefined(e("__index NOT LIKE '%tenant%'")),
                IPredicateMock.undefined(e("msg = 'some text' OR deploy in (10.10, 20.20)")),
                IPredicateMock.undefined(e("application is null")),
                IPredicateMock.undefined(e("deploy is not null")),
                IPredicateMock.undefined(e("__type != 'text'")),
                IPredicateMock.undefined(e("msg > 10")),
                IPredicateMock.undefined(e("deploy like null + '%'")),
                IPredicateMock.undefined(e("msg <= 20 OR NOT (application = 'test' and deploy = 'test2')")),
                IPredicateMock.undefined(e("msg >= CAST('2010-10-10' AS DATETIME)")),
                IPredicateMock.undefined(e("msg < CAST('2010-10-10' AS DATETIMEOFFSET)")),
                IPredicateMock.function("es", "query", List.of("some phrase")),
                IPredicateMock.function("es", "match", List.of("_index", "single match")),
                IPredicateMock.function("es", "match", List.of("_index,_id", "multi match")),
                IPredicateMock.undefined(e("nest.field > 10")),
                IPredicateMock.undefined(e("nest.field like 'test%'")),
                IPredicateMock.undefined(e("nest.field in (1,2)")),
                IPredicateMock.undefined(e("nest.field is null")),
                // Transform of a single column with a dot
                IPredicateMock.undefined(e("\"nest.field\" is not null")),
                IPredicateMock.undefined(e("msg = `some ${'hello ' + @var}`")),
                IPredicateMock.undefined(ExpressionTestUtils.ce(
                        IComparisonExpression.Type.EQUAL, ExpressionTestUtils.col(QualifiedName.of("msg")),
                        ExpressionTestUtils.function("sys", "concat", List.of(
                                e("'some'"),
                                e("@var")), "some world"))),
                IPredicateMock.undefined(ExpressionTestUtils.ce(
                        IComparisonExpression.Type.EQUAL, ExpressionTestUtils.col(QualifiedName.of("msg")),
                        ExpressionTestUtils.function("sys", "no_arg_func", List.of(), "result"))),
                IPredicateMock.undefined(e("nest.field > -@int")),
                IPredicateMock.undefined(e("msg > dateadd(hour, -1, '2010-10-10T10:10:10')")),
                IPredicateMock.undefined(e("msg < datediff(hour, '2010-10-10T10:10:10', '2010-10-11T10:10:10')")),
                IPredicateMock.undefined(e("msg < datediff(minute, '2010-10-10T10:10:10', '2010-10-11T10:10:10' at time zone 'Europe/Stockholm')")),
                IPredicateMock.undefined(e("msg <= datepart(minute, '2010-10-10T10:10:10')")),
                IPredicateMock.undefined(e("nest.field = cast(@var as string)")),
                IPredicateMock.undefined(oe("level = o.level", List.of("o.level")))
        ));
        //@formatter:on
        List<IPredicate> copyPredicates = new ArrayList<>(predicates);

        List<IPropertyPredicate> predicatesResult = ElasticQueryBuilder.collectPredicates(context.getSession(), predicates, "es", properties);

        // All predicates should be consumed
        assertTrue("Predicat indices not consumed: " + predicates.stream()
                .map(p -> String.valueOf(copyPredicates.indexOf(p)))
                .collect(joining(",")), predicates.isEmpty());

        StringBuilder filter = new StringBuilder();
        StringBuilder filterNot = new StringBuilder();

        context.setVariable("var", ValueVector.literalString("world", 1));
        context.setVariable("int", ValueVector.literalInt(1, 1));

        for (IPropertyPredicate pp : predicatesResult)
        {
            int l1 = filter.length();
            int l2 = filterNot.length();
            pp.appendBooleanClause(false, ElasticsearchMeta.Version._8X.getStrategy(), filter, filterNot, context);
            if (l1 != filter.length())
            {
                filter.append(',');
            }
            if (l2 != filterNot.length())
            {
                filterNot.append(',');
            }
        }
        if (filter.charAt(filter.length() - 1) == ',')
        {
            filter.deleteCharAt(filter.length() - 1);
        }
        if (filterNot.charAt(filterNot.length() - 1) == ',')
        {
            filterNot.deleteCharAt(filterNot.length() - 1);
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> filterActual = ESDatasource.MAPPER.readValue("[" + filter + "]", List.class);
        assertEquals(36, filterActual.size());

        assertPredicate("""
                {
                  "term" : {
                    "application" : "kappahl"
                  }
                }
                """, filterActual.get(0));
        assertPredicate("""
                {
                  "terms" : {
                    "deploy" : [ 1, 2, 3 ]
                  }
                }
                """, filterActual.get(1));
        assertPredicate("""
                {
                  "bool" : {
                    "must_not" : [ {
                      "terms" : {
                        "msg.keyword" : [ "hello", 2, 3 ]
                      }
                    } ]
                  }
                }
                """, filterActual.get(2));
        assertPredicate("""
                {
                  "bool" : {
                    "should" : [ {
                      "term" : {
                        "http.request.id" : "traceid"
                      }
                    }, {
                      "term" : {
                        "trace.id" : "traceid"
                      }
                    }, {
                      "term" : {
                        "msg.keyword" : "traceid"
                      }
                    } ]
                  }
                }
                """, filterActual.get(3));
        assertPredicate("""
                {
                  "match_none" : { }
                }
                """, filterActual.get(4));
        assertPredicate("""
                {
                  "bool" : {
                    "must_not" : [ {
                      "bool" : {
                        "should" : [ {
                          "term" : {
                            "deploy" : "traceid"
                          }
                        }, {
                          "term" : {
                            "application" : "traceid"
                          }
                        } ]
                      }
                    } ]
                  }
                }
                """, filterActual.get(5));
        assertPredicate("""
                {
                  "bool" : {
                    "should" : [ {
                      "term" : {
                        "_index" : "index1"
                      }
                    }, {
                      "match_none" : { }
                    } ]
                  }
                }
                """, filterActual.get(6));
        assertPredicate("""
                {
                  "wildcard" : {
                    "_index" : {
                      "value" : "*tenant*"
                    }
                  }
                }
                """, filterActual.get(7));
        assertPredicate("""
                {
                  "bool" : {
                    "must_not" : [ {
                      "wildcard" : {
                        "_index" : {
                          "value" : "*tenant*"
                        }
                      }
                    } ]
                  }
                }
                """, filterActual.get(8));
        assertPredicate("""
                {
                  "bool" : {
                    "should" : [ {
                      "term" : {
                        "msg.keyword" : "some text"
                      }
                    }, {
                      "terms" : {
                        "deploy" : [ 10.1, 20.2 ]
                      }
                    } ]
                  }
                }
                """, filterActual.get(9));
        assertPredicate("""
                {
                  "bool" : {
                    "must_not" : [ {
                      "exists" : {
                        "field" : "application"
                      }
                    } ]
                  }
                }
                """, filterActual.get(10));
        assertPredicate("""
                {
                  "exists" : {
                    "field" : "deploy"
                  }
                }
                """, filterActual.get(11));
        assertPredicate("""
                {
                  "bool" : {
                    "must_not" : [ {
                      "term" : {
                        "_type" : "text"
                      }
                    } ]
                  }
                }
                """, filterActual.get(12));
        assertPredicate("""
                {
                  "range" : {
                    "msg.keyword" : {
                      "gt" : 10
                    }
                  }
                }
                """, filterActual.get(13));
        assertPredicate("""
                {
                  "match_none" : { }
                }
                """, filterActual.get(14));
        assertPredicate("""
                {
                  "bool" : {
                    "should" : [ {
                      "range" : {
                        "msg.keyword" : {
                          "lte" : 20
                        }
                      }
                    }, {
                      "bool" : {
                        "must_not" : [ {
                          "bool" : {
                            "filter" : [ {
                              "term" : {
                                "application" : "test"
                              }
                            }, {
                              "term" : {
                                "deploy" : "test2"
                              }
                            } ]
                          }
                        } ]
                      }
                    } ]
                  }
                }
                """, filterActual.get(15));
        assertPredicate("""
                {
                  "range" : {
                    "msg.keyword" : {
                      "gte" : "2010-10-10T00:00:00"
                    }
                  }
                }
                """, filterActual.get(16));
        assertPredicate("""
                {
                  "range" : {
                    "msg.keyword" : {
                      "lt" : "2010-10-10T00:00:00Z"
                    }
                  }
                }
                """, filterActual.get(17));
        assertPredicate("""
                {
                  "query_string" : {
                    "query" : "some phrase"
                  }
                }
                """, filterActual.get(18));
        assertPredicate("""
                {
                  "match" : {
                    "_index" : "single match"
                  }
                }
                """, filterActual.get(19));
        assertPredicate("""
                {
                  "multi_match" : {
                    "fields" : [ "_index", "_id" ],
                    "query" : "multi match"
                  }
                }
                """, filterActual.get(20));
        assertPredicate("""
                {
                  "nested" : {
                    "path" : "nest",
                    "query" : {
                      "range" : {
                        "nest.field" : {
                          "gt" : 10
                        }
                      }
                    }
                  }
                }
                """, filterActual.get(21));
        assertPredicate("""
                {
                  "nested" : {
                    "path" : "nest",
                    "query" : {
                      "wildcard" : {
                        "nest.field" : {
                          "value" : "test*"
                        }
                      }
                    }
                  }
                }
                """, filterActual.get(22));
        assertPredicate("""
                {
                  "nested" : {
                    "path" : "nest",
                    "query" : {
                      "terms" : {
                        "nest.field" : [ 1, 2 ]
                      }
                    }
                  }
                }
                """, filterActual.get(23));
        assertPredicate("""
                {
                  "bool" : {
                    "must_not" : [ {
                      "nested" : {
                        "path" : "nest",
                        "query" : {
                          "exists" : {
                            "field" : "nest.field"
                          }
                        }
                      }
                    } ]
                  }
                }
                """, filterActual.get(24));
        assertPredicate("""
                {
                  "nested" : {
                    "path" : "nest",
                    "query" : {
                      "exists" : {
                        "field" : "nest.field"
                      }
                    }
                  }
                }
                """, filterActual.get(25));
        assertPredicate("""
                {
                  "term" : {
                    "msg.keyword" : "some hello world"
                  }
                }
                """, filterActual.get(26));
        assertPredicate("""
                {
                  "term" : {
                    "msg.keyword" : "some world"
                  }
                }
                """, filterActual.get(27));
        assertPredicate("""
                {
                  "term" : {
                    "msg.keyword" : "result"
                  }
                }
                """, filterActual.get(28));
        assertPredicate("""
                {
                  "nested" : {
                    "path" : "nest",
                    "query" : {
                      "range" : {
                        "nest.field" : {
                          "gt" : -1
                        }
                      }
                    }
                  }
                }
                """, filterActual.get(29));
        assertPredicate("""
                {
                  "range" : {
                    "msg.keyword" : {
                      "gt" : "2010-10-10T09:10:10"
                    }
                  }
                }
                """, filterActual.get(30));
        assertPredicate("""
                {
                  "range" : {
                    "msg.keyword" : {
                      "lt" : 24
                    }
                  }
                }
                """, filterActual.get(31));
        assertPredicate("""
                {
                  "range" : {
                    "msg.keyword" : {
                      "lt" : 1440
                    }
                  }
                }
                """, filterActual.get(32));
        assertPredicate("""
                {
                  "range" : {
                    "msg.keyword" : {
                      "lte" : 10
                    }
                  }
                }
                """, filterActual.get(33));
        assertPredicate("""
                                {
                  "nested" : {
                    "path" : "nest",
                    "query" : {
                      "term" : {
                        "nest.field" : "world"
                      }
                    }
                  }
                }
                """, filterActual.get(34));
        assertPredicate("""
                {
                  "term" : {
                    "level" : "o.level"
                  }
                }
                """, filterActual.get(35));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> filterNotActual = ESDatasource.MAPPER.readValue("[" + filterNot + "]", List.class);
        assertEquals(2, filterNotActual.size());

        assertPredicate("""
                {
                  "bool" : {
                    "filter" : [ {
                      "term" : {
                        "application" : "app1"
                      }
                    }, {
                      "term" : {
                        "deploy" : "stage"
                      }
                    } ]
                  }
                }
                """, filterNotActual.get(0));
        assertPredicate("""
                {
                  "bool" : {
                    "filter" : [ {
                      "term" : {
                        "application" : "app2"
                      }
                    }, {
                      "term" : {
                        "deploy" : "prod"
                      }
                    } ]
                  }
                }
                """, filterNotActual.get(1));
    }

    @Test
    public void test_literal_or_variable_predicate()
    {
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("null")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("1")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("1L")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("1F")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("1D")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("'hello'")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("false")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("@var")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("CAST(1 as decimal)")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("CAST('2010-10-10' as datetime)")));
        assertTrue(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("CAST('2010-10-10' as datetimeoffset)")));
        assertFalse(ElasticQueryBuilder.PredicateCheckerVisitor.IS_LITERAL_OR_VARIABLE.test(e("@var > 10")));
    }

    private IExpression e(String expression)
    {
        return PARSER.parseExpression(expression);
    }

    /** Create an outer reference expression. */
    private IExpression oe(String expression, List<String> columnsToMakeOuter)
    {
        IExpression e = e(expression);
        return e.accept(new ARewriteExpressionVisitor<Void>()
        {
            @Override
            public IExpression visit(UnresolvedColumnExpression expression, Void context)
            {
                String column = expression.getColumn()
                        .toDotDelimited()
                        .toLowerCase();
                if (columnsToMakeOuter.contains(column))
                {
                    UnresolvedColumnExpression spy = Mockito.spy(expression);
                    when(spy.isOuterReference()).thenReturn(true);
                    doReturn(ValueVector.literalAny(1, column)).when(spy)
                            .eval(any(IExecutionContext.class));
                    return spy;
                }
                return expression;
            }
        }, null);
    }

    private void assertPredicate(String expected, Map<String, Object> actual) throws JsonMappingException, JsonProcessingException
    {
        assertEquals(MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(MAPPER.readValue(expected, Map.class)),
                MAPPER.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(actual));
    }
}
