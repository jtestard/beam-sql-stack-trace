package dummy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Main
{
    private static void runPipeline(PipelineOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);
        BuildPipeline(p);
        p.run().waitUntilFinish();
    }

    public static void BuildPipeline(Pipeline p) throws IOException {
        PCollection<Row> input = p.apply(Create.of(rows)).setRowSchema(demoSchema);
        PCollection<Row> output = input.apply("Transform1", SqlTransform.query("SELECT A, B from PCOLLECTION")).setRowSchema(outputSchema);
        PCollection<Row> output2 = output.apply("Transform2", SqlTransform.query("SELECT A, B from PCOLLECTION")).setRowSchema(demoSchema);
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        options.setRunner(org.apache.beam.runners.direct.DirectRunner.class);
        try {
            runPipeline(options);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final Schema demoSchema = Schema.builder()
            .addStringField("A")
            .addNullableField("B", Schema.FieldType.STRING)
            .build();

    private static final Schema outputSchema = Schema.builder()
            .addStringField("A")
            .addStringField("B")
            .build();

    private static List<Row> rows = Arrays.asList(
            Row.withSchema(demoSchema).addValues("foo", "bar").build(),
            Row.withSchema(demoSchema).addValues("foo", null).build()
    );
}
