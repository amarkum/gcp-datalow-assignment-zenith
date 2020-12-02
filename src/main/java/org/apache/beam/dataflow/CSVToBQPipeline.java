package org.apache.beam.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
/**
 * <p>
 * Dependency of Java DirectRunner is added, if no command line arguments are passed,
 * it will run directly on the machine
 *
 * To use Dataflow Runner, pass below to the command line argument
 * --project=<YOUR_PROJECT_ID> --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 *
 */

public class CSVToBQPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(CSVToBQPipeline.class);
    private static String HEADERS = "TIMESTAMP,CUSTOMER,POINTNAME,VALUE";
    public static class FormatForBigquery extends DoFn<String, TableRow> {
        private String[] columnNames = HEADERS.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = new TableRow();
            String[] parts = c.element().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (!c.element().contains(HEADERS)) {
                for (int i = 0; i < parts.length; i++) {
                    row.set(columnNames[i], parts[i]);
                }
                c.output(row);
            }
        }

        static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();

            fields.add(new TableFieldSchema().setName("TIMESTAMP").setType("DATE"));
            fields.add(new TableFieldSchema().setName("CUSTOMER").setType("STRING"));
            fields.add(new TableFieldSchema().setName("POINTNAME").setType("STRING"));
            fields.add(new TableFieldSchema().setName("VALUE").setType("FLOAT"));

            return new TableSchema().setFields(fields);
        }
    }
    public static void main(String[] args) throws Throwable {

        String sourceFilePath = "gs://dc-rh-bucketdata/data/2018/10/rh_201811.csv";
        String tempLocationPath = "gs://dc-rh-bucketdata/tmp";
        boolean isStreaming = false;
        TableReference tableRef = new TableReference();

        tableRef.setProjectId("gentle-mapper-296705");
        tableRef.setDatasetId("real_estate_leads");
        tableRef.setTableId("dc_rh");
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

        options.setTempLocation(tempLocationPath);
        options.setJobName("csv-to-bigquery");
        Pipeline p = Pipeline.create(options);
        p.apply("Read CSV File", TextIO.read().from(sourceFilePath))
                .apply("Log messages", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("Processing row: " + c.element());
                        c.output(c.element());
                    }
                })).apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))
                .apply("Write into BigQuery",
                        BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatForBigquery.getSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(isStreaming ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
                                        : BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        p.run().waitUntilFinish();
    }
}
