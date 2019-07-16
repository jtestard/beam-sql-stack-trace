package dummy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Test;

import java.io.IOException;

public class MainTest {
    @Test
    public void testMain() throws IOException {
        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions options = PipelineOptionsFactory.fromArgs().withValidation().as(PipelineOptions.class);
        Pipeline p = TestPipeline.create(options);
        Main.BuildPipeline(p);
        p.run().waitUntilFinish();
    }
}
