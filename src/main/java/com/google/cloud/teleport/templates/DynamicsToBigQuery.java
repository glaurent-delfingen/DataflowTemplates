/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// mvn compile exec:java -Dexec.mainClass=com.google.cloud.teleport.templates.DynamicsToBigQuery -Dexec.cleanupDaemonThreads=false -Dexec.args=" --project=delfingen-prod-bi  --stagingLocation=gs://delfingen-prod-bi-dataflow/staging --tempLocation=gs://delfingen-prod-bi-dataflow/temp --templateLocation=gs://delfingen-prod-bi-dataflow/templates/template_dataflow_dynamics.json  --runner=DataflowRunner --region=europe-west1"

package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.templates.common.BigQueryConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
//import java.util.regex.*;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * Templated pipeline to read text from TextIO, apply a javascript UDF to it, and write it to GCS.
 */
public class DynamicsToBigQuery {

    /** Options supported by {@link TextIOToBigQuery}. */
    public interface Options extends DataflowPipelineOptions, JavascriptTextTransformerOptions {
        @Description("The GCS location of the text you'd like to process")
        ValueProvider<String> getInputFilePattern();

        void setInputFilePattern(ValueProvider<String> value);

        @Description("JSON file with BigQuery Schema description")
        ValueProvider<String> getJSONPath();

        void setJSONPath(ValueProvider<String> value);

        @Description("Output topic to write to")
        ValueProvider<String> getOutputTable();

        void setOutputTable(ValueProvider<String> value);

        @Description("GCS path to javascript fn for transforming output")
        ValueProvider<String> getJavascriptTextTransformGcsPath();

        void setJavascriptTextTransformGcsPath(ValueProvider<String> jsTransformPath);

        @Validation.Required
        @Description("UDF Javascript Function Name")
        ValueProvider<String> getJavascriptTextTransformFunctionName();

        void setJavascriptTextTransformFunctionName(
                ValueProvider<String> javascriptTextTransformFunctionName);

        @Validation.Required
        @Description("Temporary directory for BigQuery loading process")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();
        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);



    }

    //private static final Logger LOG = LoggerFactory.getLogger(TextIOToBigQuery.class);

    private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String MODE = "mode";

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read from source", TextIO.read().from(options.getInputFilePattern()))
                .apply(ParDo.of(new DynamicsToBigQuery.RemoveHeader()))
                .apply(
                        TransformTextViaJavascript.newBuilder()
                                .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                                .setFunctionName(options.getJavascriptTextTransformFunctionName())
                                .build())
                .apply(BigQueryConverters.jsonToTableRow())
                .apply(
                        "Insert into Bigquery",
                        BigQueryIO.writeTableRows()
                                .withSchema(
                                        NestedValueProvider.of(
                                                options.getJSONPath(),
                                                new SerializableFunction<String, TableSchema>() {

                                                    @Override
                                                    public TableSchema apply(String jsonPath) {

                                                        TableSchema tableSchema = new TableSchema();
                                                        List<TableFieldSchema> fields = new ArrayList<>();
                                                        SchemaParser schemaParser = new SchemaParser();
                                                        JSONObject jsonSchema;

                                                        try {

                                                            jsonSchema = schemaParser.parseSchema(jsonPath);

                                                            JSONArray bqSchemaJsonArray =
                                                                    jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

                                                            for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
                                                                JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
                                                                TableFieldSchema field =
                                                                        new TableFieldSchema()
                                                                                .setName(inputField.getString(NAME))
                                                                                .setType(inputField.getString(TYPE));

                                                                if (inputField.has(MODE)) {
                                                                    field.setMode(inputField.getString(MODE));
                                                                }

                                                                fields.add(field);
                                                            }
                                                            tableSchema.setFields(fields);

                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                        return tableSchema;
                                                    }
                                                }))
                                .to(options.getOutputTable())
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .ignoreUnknownValues()
                                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

        pipeline.run();
    }



    static class RemoveHeader extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String line;
            if (  (c.element().trim().contains( "JOURNALBATCHNUMBER;LINENUMBER;ACCOUNTDISPLAYVALUE"))      ){
                // header ledger
                //System.out.println(c.element().trim() );
                //System.out.println(c.element().trim().substring(0,2) );

            } else if ((c.element().trim().substring(0,2).equals("SK") ) || (c.element().trim().substring(0,2).equals("FR") ) || (c.element().trim().substring(0,2).equals("MA")) || (c.element().trim().substring(0,2).equals("TN")) || (c.element().trim().substring(0,2).equals("RO")) || (c.element().trim().substring(0,2).equals("US")) || (c.element().trim().substring(0,2).equals("DE")) || (c.element().trim().substring(0,2).equals("PT"))) {
                // contenu ledger
                line = c.element();

                // Corrections manuelles pour supprimer les ";" de l'export
                if ((c.element().trim().substring(0,2).equals("FR")) && (c.element().trim().contains("VIR CERRI;COM")    ))  {
                    line = line.replace("VIR CERRI;COM", "VIR CERRI.COM");
                }
                // Check si 51 ; (ok) ou + (NOK)
                int count = StringUtils.countMatches(line, ";");
                if (count <= 51 ) {
                    c.output(line);
                } else {
                    System.out.println("Error for line " + line);
                    if  (count >= 52 ) {
                        String[] tab = line.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                        String textfield = tab[48];
                        String descriptionfield = tab[15];
                        tab[48] = textfield.replace(";",",");
                        tab[15] = descriptionfield.replace(";",",");
                        System.out.println("Error for line field " + textfield + " or " + descriptionfield);
                        String newline = String.join(";", tab);
                        System.out.println("Error for line field " + newline);
                        c.output(newline);
                    }




                }


                //System.out.println(c.element().trim() );
            } else if  (  (c.element().trim().contains( "CUSTTRANSRECID;ACCOUNTINGCURRENCY;AMOUNTCUR;AMOUNTMST;COMPANY;CURRENCYCODE;DUEDATE;INVOICE;INVOICEDATE;VOUCHER"))      ){
                // header past due

            } else if ((c.element().trim().contains("de65") )  || (c.element().trim().contains("sk08") )  || (c.element().trim().contains("fr10") )  ||  (c.element().trim().contains("fr22")) || (c.element().trim().contains("fr30")) || (c.element().trim().contains("ma02")) || (c.element().trim().contains("pt46")) || (c.element().trim().contains("ro27")) || (c.element().trim().contains("ro18")) || (c.element().trim().contains("fr30")) || (c.element().trim().contains("tn09"))) {
                // contenu past due
                line = c.element();
                c.output(line);
                //System.out.println(c.element().trim() );

            }



        }
    }
}
