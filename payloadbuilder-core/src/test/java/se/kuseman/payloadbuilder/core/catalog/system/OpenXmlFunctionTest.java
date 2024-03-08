package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.core.physicalplan.DatasourceOptions;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link OpenXmlFunction} */
public class OpenXmlFunctionTest extends APhysicalPlanTest
{
    private final TableFunctionInfo f = SystemCatalog.get()
            .getTableFunction("openxml");

    @Test
    public void test_empty_on_null()
    {
        assertEquals(Arity.ONE, f.arity());
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("null")), new DatasourceOptions(emptyList()));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_empty_xml()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                '<xml />'
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, e("'/'")))));

        assertFalse(it.hasNext());
        try
        {
            it.next();
            fail("Should throw NoSuchElementException");
        }
        catch (NoSuchElementException e)
        {
        }
        it.close();
    }

    @Test
    public void test_empty_xml_node_but_with_attributes()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                '<xml attr1="1234" />'
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, e("'/'")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("xml_attr1", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "1234")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_batch_size()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <key>123</key>
                      <key2>456</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record")), new Option(DatasourceOptions.BATCH_SIZE, intLit(1)))));

        int batchCount = 0;
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            batchCount++;

            if (batchCount == 1)
            {
                //@formatter:off
                VectorTestUtils.assertTupleVectorsEquals(
                        TupleVector.of(Schema.of(Column.of("key", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                                VectorTestUtils.vv(Column.Type.Any, "123"),
                                VectorTestUtils.vv(Column.Type.Any, "456")
                                )), next);
                //@formatter:on
            }
            else
            {
                //@formatter:off
                VectorTestUtils.assertTupleVectorsEquals(
                        TupleVector.of(Schema.of(Column.of("key", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                                VectorTestUtils.vv(Column.Type.Any, "1230"),
                                VectorTestUtils.vv(Column.Type.Any, "4560")
                                )), next);
                //@formatter:on
            }
            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
        assertEquals(2, batchCount);
    }

    @Test
    public void test_batch_size_with_only_new_columns()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <key>123</key>
                      <key2>456</key2>
                  </record>
                  <record>
                      <key3>4560</key3>
                      <key4>1230</key4>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record")), new Option(DatasourceOptions.BATCH_SIZE, intLit(1)))));

        int batchCount = 0;
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            batchCount++;

            if (batchCount == 1)
            {
                //@formatter:off
                VectorTestUtils.assertTupleVectorsEquals(
                        TupleVector.of(Schema.of(Column.of("key", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                                VectorTestUtils.vv(Column.Type.Any, "123"),
                                VectorTestUtils.vv(Column.Type.Any, "456")
                                )), next);
                //@formatter:on
            }
            else
            {
                //@formatter:off
                VectorTestUtils.assertTupleVectorsEquals(
                        TupleVector.of(Schema.of(Column.of("key3", Column.Type.Any), Column.of("key4", Column.Type.Any)), List.of(
                                VectorTestUtils.vv(Column.Type.Any, "4560"),
                                VectorTestUtils.vv(Column.Type.Any, "1230")
                                )), next);
                //@formatter:on
            }
            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
        assertEquals(2, batchCount);
    }

    @Test
    public void test_complex_xml_cdata()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                                `<records>
                                  <record>
                                      <value attr1="666">123</value>
                                      <value1><![CDATA[ <xml> ]]> </value1>
                                      <value2>
                                         <subElement attribute1 ="123">hello world</subElement>
                                        <value3><![CDATA[
                Within this Character Data block I can
                use double dashes as much as I want (along with <, &, ', and ")
                *and* %MyParamEntity; will be expanded to the text
                "Has been expanded" ... however, I can't use
                the CEND sequence. If I need to use CEND I must escape one of the
                brackets or the greater-than sign using concatenated CDATA sections.
                ]]></value3>
                                      </value2>
                                  </record>
                                  <record>
                                    <value attr1="1337">789</value>
                                    <value1>10-20-20</value1>
                                  </record>

                                </records>
                                `
                                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(
                            Column.of("value", Column.Type.Any),
                            Column.of("value_attr1", Column.Type.Any),
                            Column.of("value1", Column.Type.Any),
                            Column.of("value2", Column.Type.Any)
                            ), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "123", "789"),
                            VectorTestUtils.vv(Column.Type.Any, "666", "1337"),
                            VectorTestUtils.vv(Column.Type.Any, " <xml> ", "10-20-20"),
                            VectorTestUtils.vv(Column.Type.Any, """
                                    <value2><subElement attribute1='123'>hello world</subElement><value3>
                                    Within this Character Data block I can
                                    use double dashes as much as I want (along with <, &, ', and ")
                                    *and* %MyParamEntity; will be expanded to the text
                                    "Has been expanded" ... however, I can't use
                                    the CEND sequence. If I need to use CEND I must escape one of the
                                    brackets or the greater-than sign using concatenated CDATA sections.
                                    </value3></value2>""", null))
                            ), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <KEY some_fancy_attribute="10.10">123</KEY>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("KEY", Column.Type.Any), Column.of("KEY_some_fancy_attribute", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "123", "1230"),
                            VectorTestUtils.vv(Column.Type.Any, "10.10", null),
                            VectorTestUtils.vv(Column.Type.Any, "1230", "4560")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_columns_option()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <KEY some_fancy_attribute="10.10">123</KEY>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(
                List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record")), new Option(OpenXmlFunction.XMLCOLUMNS, new LiteralStringExpression("KEY,non_column")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("KEY", Column.Type.Any), Column.of("non_column", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "123"),
                            VectorTestUtils.vv(Column.Type.Any, new Object[] { null })
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_columns_option_2()
    {
        // Columns should come in the order specified in options and not in the order present in XML
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <KEY some_fancy_attribute="10.10">123</KEY>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                      <key3>hello world</key3>

                  </record>
                </records>
                `
                """)), new DatasourceOptions(
                List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record")), new Option(OpenXmlFunction.XMLCOLUMNS, new LiteralStringExpression("key3,key2")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key3", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, null, "hello world"),
                            VectorTestUtils.vv(Column.Type.Any, "1230", "4560")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_pointer_to_a_deep_nested_element()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                      <record>
                          <prop>123</prop>
                          <subXml>
                             <values>
                                <value>1</value>
                                <value>2</value>
                                <value>3</value>
                             </values>
                          </subXml>
                      </record>
                      <record>
                          <prop>456</prop>
                      </record>
                </records>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record/subXml/values")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("value", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, vv(Type.Any, "1", "2", "3"))
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_no_path()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <KEY some_fancy_attribute="10.10">123</KEY>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(List.of()));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            //CSOFF
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("records", Column.Type.Any)), List.of(
                        VectorTestUtils.vv(Column.Type.Any, "<records><record><KEY some_fancy_attribute='10.10'>123</KEY><key2>1230</key2></record><record><key2>4560</key2><key>1230</key></record></records>")
                    )), next);
            //CSON
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_root_path()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <KEY some_fancy_attribute="10.10">123</KEY>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            //CSOFF
            VectorTestUtils.assertTupleVectorsEquals(
                TupleVector.of(Schema.of(Column.of("records", Column.Type.Any)), List.of(
                    VectorTestUtils.vv(Column.Type.Any, "<records><record><KEY some_fancy_attribute='10.10'>123</KEY><key2>1230</key2></record><record><key2>4560</key2><key>1230</key></record></records>")
            )), next);
            //CSON
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_totally_different_columns_on_rows()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <key>123</key>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key3>4560</key3>
                      <key4>1230</key4>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(
                            Column.of("key", Column.Type.Any),
                            Column.of("key2", Column.Type.Any),
                            Column.of("key3", Column.Type.Any),
                            Column.of("key4", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "123", null),
                            VectorTestUtils.vv(Column.Type.Any, "1230", null),
                            VectorTestUtils.vv(Column.Type.Any, null, "4560"),
                            VectorTestUtils.vv(Column.Type.Any, null, "1230")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_not_found_path()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <KEY some_fancy_attribute="10.10">123</KEY>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/articles/article")))));

        assertFalse(it.hasNext());
    }

    @Test
    public void test_xml_path_to_top_element()
    {
        // Here we will get an array of values since <record> exists multiple times
        // under the xmlpath
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <KEY some_fancy_attribute="10.10">123</KEY>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            //CSOFF
            VectorTestUtils.assertTupleVectorsEquals(
                TupleVector.of(Schema.of(Column.of("record", Column.Type.Any)), List.of(
                    VectorTestUtils.vv(Column.Type.Any, vv(Type.Any, "<record><KEY some_fancy_attribute='10.10'>123</KEY><key2>1230</key2></record>", "<record><key2>4560</key2><key>1230</key></record>"))
            )), next);
            //CSON
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_arrays()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <campaigns>
                      <value>camp1</value>
                      <value>camp2</value>
                      <value>camp3</value>
                      <value>camp4</value>
                  </campaigns>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/campaigns")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("value", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, vv(Type.Any, "camp1", "camp2", "camp3", "camp4"))
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_arrays_from_attributes()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <campaigns>
                      <value attr="camp1" />
                      <value attr="camp2" />
                      <value attr="camp3" />
                      <value attr="camp4" />
                  </campaigns>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/campaigns")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("value_attr", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, vv(Type.Any, "camp1", "camp2", "camp3", "camp4"))
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_single_scalar_value_pointer()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <campaigns>
                      <value>camp1</value>
                      <value>camp2</value>
                      <value>camp3</value>
                      <value>camp4</value>
                  </campaigns>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/campaigns/value")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("value", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "camp1")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_single_scalar_value_pointer_2()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <campaign>
                      <name>camp1</name>
                      <startDate>2010-10-10</startDate>
                      <market>SE</market>
                  </campaign>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/campaign/startDate")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("startDate", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "2010-10-10")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_single_scalar_value_pointer_3()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <campaign>
                      <name>camp1</name>
                      <variants>
                          <variant>12345</variant>
                          <variant>12346</variant>
                          <variant>12347</variant>
                          <variant>12348</variant>
                      </variants>
                      <startDate>2010-10-10</startDate>
                      <market>SE</market>
                  </campaign>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/campaign/startDate")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("startDate", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "2010-10-10")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_new_columns_second_row()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("""
                `<?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <key>123</key>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key>4567</key>
                      <key3 attr="123">hello world</key3>
                  </record>
                </records>
                `
                """)), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(
                            Column.of("key", Column.Type.Any),
                            Column.of("key2", Column.Type.Any),
                            Column.of("key3", Column.Type.Any),
                            Column.of("key3_attr", Column.Type.Any)),
                            List.of(
                                VectorTestUtils.vv(Column.Type.Any, "123", "4567"),
                                VectorTestUtils.vv(Column.Type.Any, "1230", null),
                                VectorTestUtils.vv(Column.Type.Any, null, "hello world"),
                                VectorTestUtils.vv(Column.Type.Any, null, "123")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_product()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null),
                asList(e(
                        """
                                `<?xml version="1.0" encoding="UTF-8"?>
                                  <records xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                                  <record>
                                    <id>1757248-01</id>
                                    <name>Ljuskälla Kronljus E14 LED</name>
                                    <NameSeo>ljuskalla-kronljus-e14-led</NameSeo>
                                    <ProductSalePriceNet>59</ProductSalePriceNet>
                                    <ProductListPrice>59</ProductListPrice>
                                    <ProductSalePriceNetFormatted>59</ProductSalePriceNetFormatted>
                                    <ProductListPriceFormatted>59</ProductListPriceFormatted>
                                    <PriceDiscount>0</PriceDiscount>
                                    <brand>Aneta Lighting:aneta-lighting</brand>
                                    <SubBrand>Aneta Lighting</SubBrand>
                                    <SubBrandSeo>aneta-lighting</SubBrandSeo>
                                    <category1>Hem &amp; inredning:hem-inredning:false</category1>
                                    <category2>Belysning &amp; lampor:hem-inredning/belysning-lampor:false</category2>
                                    <category3>Ljuskällor:hem-inredning/belysning-lampor/ljuskallor:false</category3>
                                    <features>Unisex</features>
                                    <avgRating xsi:nil="true"/>
                                    <BaseColor>Transparent:transparent</BaseColor>
                                    <Color_of_product>Transparent</Color_of_product>
                                    <Campaigns>bw-brand-deal</Campaigns>
                                    <Seasons>2023AW</Seasons>
                                    <Discountable>yes</Discountable>
                                    <defaultImageFront>hom_1757248-01_Fs</defaultImageFront>
                                    <defaultImageAlternative>hom_1757248-01_Fs</defaultImageAlternative>
                                    <StockQuantity>54</StockQuantity>
                                    <ProductLabels><![CDATA[{"2":[{"id":"label_EnergyF6ELL_se","prio":1}]}]]></ProductLabels>
                                    <freshness>138</freshness>
                                    <margin>nono</margin>
                                    <visibility>5</visibility>
                                    <SkusData><![CDATA[[{"sku":"1757248-01-0","ean":"7041661272019","url":"https://www.ellos.se/aneta-lighting/ljuskalla-kronljus-e14-led/1757248-01-0","currentPriceBasicFormatted":"59.00","originalPriceBasicFormatted":"59.00","stockQuantity":54,"stockType":0,"cost":17.7,"isInStock":true}]]]></SkusData>
                                    <Seasonality>NOOS</Seasonality>
                                    <stockCompletenessLevel1>1,00</stockCompletenessLevel1>
                                    <stockCompletenessLevel2>1,00</stockCompletenessLevel2>
                                    <isDropship>yes</isDropship>
                                    <description>Kronljus e14 2w led filament 2700k, klar</description>
                                    <list>
                                        <item>
                                            <sku>1757248-01-0</sku>
                                            <isInStock>true</isInStock>
                                        </item>
                                    </list>
                                    <width>4</width>
                                    <depth>4</depth>
                                    <height>10</height>
                                    <spannedPrice>0</spannedPrice>
                                </record>
                                </records>
                                `
                                """)),
                new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, new LiteralStringExpression("/records/record")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(
                            Column.of("id", Column.Type.Any),
                            Column.of("name", Column.Type.Any),
                            Column.of("NameSeo", Column.Type.Any),
                            Column.of("ProductSalePriceNet", Column.Type.Any),
                            Column.of("ProductListPrice", Column.Type.Any),
                            Column.of("ProductSalePriceNetFormatted", Column.Type.Any),
                            Column.of("ProductListPriceFormatted", Column.Type.Any),
                            Column.of("PriceDiscount", Column.Type.Any),
                            Column.of("brand", Column.Type.Any),
                            Column.of("SubBrand", Column.Type.Any),
                            Column.of("SubBrandSeo", Column.Type.Any),
                            Column.of("category1", Column.Type.Any),
                            Column.of("category2", Column.Type.Any),
                            Column.of("category3", Column.Type.Any),
                            Column.of("features", Column.Type.Any),
                            Column.of("BaseColor", Column.Type.Any),
                            Column.of("Color_of_product", Column.Type.Any),
                            Column.of("Campaigns", Column.Type.Any),
                            Column.of("Seasons", Column.Type.Any),
                            Column.of("Discountable", Column.Type.Any),
                            Column.of("defaultImageFront", Column.Type.Any),
                            Column.of("defaultImageAlternative", Column.Type.Any),
                            Column.of("StockQuantity", Column.Type.Any),
                            Column.of("ProductLabels", Column.Type.Any),
                            Column.of("freshness", Column.Type.Any),
                            Column.of("margin", Column.Type.Any),
                            Column.of("visibility", Column.Type.Any),
                            Column.of("SkusData", Column.Type.Any),
                            Column.of("Seasonality", Column.Type.Any),
                            Column.of("stockCompletenessLevel1", Column.Type.Any),
                            Column.of("stockCompletenessLevel2", Column.Type.Any),
                            Column.of("isDropship", Column.Type.Any),
                            Column.of("description", Column.Type.Any),
                            Column.of("list", Column.Type.Any),
                            Column.of("width", Column.Type.Any),
                            Column.of("depth", Column.Type.Any),
                            Column.of("height", Column.Type.Any),
                            Column.of("spannedPrice", Column.Type.Any)
                            ), List.of(
                            vv(Column.Type.Any, "1757248-01"),
                            vv(Column.Type.Any, "Ljuskälla Kronljus E14 LED"),
                            vv(Column.Type.Any, "ljuskalla-kronljus-e14-led"),
                            vv(Column.Type.Any, "59"),
                            vv(Column.Type.Any, "59"),
                            vv(Column.Type.Any, "59"),
                            vv(Column.Type.Any, "59"),
                            vv(Column.Type.Any, "0"),
                            vv(Column.Type.Any, "Aneta Lighting:aneta-lighting"),
                            vv(Column.Type.Any, "Aneta Lighting"),
                            vv(Column.Type.Any, "aneta-lighting"),
                            vv(Column.Type.Any, "Hem & inredning:hem-inredning:false"),
                            vv(Column.Type.Any, "Belysning & lampor:hem-inredning/belysning-lampor:false"),
                            vv(Column.Type.Any, "Ljuskällor:hem-inredning/belysning-lampor/ljuskallor:false"),
                            vv(Column.Type.Any, "Unisex"),
                            vv(Column.Type.Any, "Transparent:transparent"),
                            vv(Column.Type.Any, "Transparent"),
                            vv(Column.Type.Any, "bw-brand-deal"),
                            vv(Column.Type.Any, "2023AW"),
                            vv(Column.Type.Any, "yes"),
                            vv(Column.Type.Any, "hom_1757248-01_Fs"),
                            vv(Column.Type.Any, "hom_1757248-01_Fs"),
                            vv(Column.Type.Any, "54"),
                            vv(Column.Type.Any, "{\"2\":[{\"id\":\"label_EnergyF6ELL_se\",\"prio\":1}]}"),
                            vv(Column.Type.Any, "138"),
                            vv(Column.Type.Any, "nono"),
                            vv(Column.Type.Any, "5"),
                            vv(Column.Type.Any, "[{\"sku\":\"1757248-01-0\",\"ean\":\"7041661272019\",\"url\":\"https://www.ellos.se/aneta-lighting/ljuskalla-kronljus-e14-led/1757248-01-0\",\"currentPriceBasicFormatted\":\"59.00\",\"originalPriceBasicFormatted\":\"59.00\",\"stockQuantity\":54,\"stockType\":0,\"cost\":17.7,\"isInStock\":true}]"),
                            vv(Column.Type.Any, "NOOS"),
                            vv(Column.Type.Any, "1,00"),
                            vv(Column.Type.Any, "1,00"),
                            vv(Column.Type.Any, "yes"),
                            vv(Column.Type.Any, "Kronljus e14 2w led filament 2700k, klar"),
                            vv(Column.Type.Any, "<list><item><sku>1757248-01-0</sku><isInStock>true</isInStock></item></list>"),
                            vv(Column.Type.Any, "4"),
                            vv(Column.Type.Any, "4"),
                            vv(Column.Type.Any, "10"),
                            vv(Column.Type.Any, "0")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }

        try
        {
            it.next();
            fail("No such element");
        }
        catch (NoSuchElementException e)
        {
        }

        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_reader_gets_picked_up()
    {
        MutableBoolean closed = new MutableBoolean(false);
        IExpression arg = Mockito.mock(IExpression.class);
        StringReader reader = new StringReader("""
                <?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <KEY some_fancy_attribute="10.10">123</KEY>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                  </record>
                </records>
                """)
        {
            @Override
            public void close()
            {
                closed.setTrue();
                super.close();
            }
        };

        Mockito.when(arg.eval(Mockito.any()))
                .thenReturn(VectorTestUtils.vv(Column.Type.Any, reader));

        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(arg), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, e("'/records/record'")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("KEY", Column.Type.Any), Column.of("KEY_some_fancy_attribute", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "123", "1230"),
                            VectorTestUtils.vv(Column.Type.Any, "10.10", null),
                            VectorTestUtils.vv(Column.Type.Any, "1230", "4560")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
        assertTrue(closed.isTrue());
    }

    @Test
    public void test_inputstream_gets_picked_up()
    {
        MutableBoolean closed = new MutableBoolean(false);
        IExpression arg = Mockito.mock(IExpression.class);
        ByteArrayInputStream baos = new ByteArrayInputStream("""
                <?xml version="1.0" encoding="UTF-8"?>
                  <records>
                  <record>
                      <KEY some_fancy_attribute="10.10">123</KEY>
                      <key2>1230</key2>
                  </record>
                  <record>
                      <key2>4560</key2>
                      <key>1230</key>
                  </record>
                </records>
                """.getBytes(StandardCharsets.UTF_8))
        {
            @Override
            public void close() throws IOException
            {
                closed.setTrue();
                super.close();
            }
        };

        Mockito.when(arg.eval(Mockito.any()))
                .thenReturn(VectorTestUtils.vv(Column.Type.Any, baos));

        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(arg), new DatasourceOptions(List.of(new Option(OpenXmlFunction.XMLPATH, e("'/records/record'")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("KEY", Column.Type.Any), Column.of("KEY_some_fancy_attribute", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, "123", "1230"),
                            VectorTestUtils.vv(Column.Type.Any, "10.10", null),
                            VectorTestUtils.vv(Column.Type.Any, "1230", "4560")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
        assertTrue(closed.isTrue());
    }
}
