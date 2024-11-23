package inverted;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * This class implements a Hadoop MapReduce job to invert an index.
 * It takes a CSV input where the first column is the key 
 * and subsequent columns are values. The job outputs an inverted
 * index where each term is associated with the list of document IDs that contain it.
 */
public class IndexInverterJob extends Configured implements Tool {

    /**
     * Mapper class to process input lines and emit term-document pairs.
     */
    public static class IndexInverterMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private static final Logger logger = Logger.getLogger(IndexInverterMapper.class);

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        
        /**
         * Maps input lines to intermediate term-document pairs.
         * Each line is split by commas. The first value is treated as the document ID,
         * and the rest are treated as terms. For each term, a key-value pair
         * is emitted with the term as the key and the document ID as the value.
         *
         * @param key Input key.
         * @param value Input value.
         * @param context Context object for writing output.
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                logger.info("Processing line: " + value.toString());
                String[] words = value.toString().split(",");
                outputValue.set(words[0]); // Document ID
                for (int i = 1; i < words.length; i++) {
                    outputKey.set(words[i]); // Term
                    context.write(outputKey, outputValue);
                    logger.info("Emitted pair: (" + outputKey + ", " + outputValue + ")");
                }
            } catch (Exception e) {
                logger.error("Error processing line: " + value.toString(), e);
            }
        }
    }
    
    /**
     * Reducer class to aggregate document IDs for each term.
     */
    public static class IndexInverterReducer extends
            Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();
         
        /**
         * Reduces term-document pairs to produce the inverted index.
         * For each term, aggregates all document IDs into a comma-separated list.
         *
         * @param key Term.
         * @param values Iterable list of document IDs.
         * @param context Context object for writing output.
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            try {
                StringBuilder builder = new StringBuilder();
                for (Text value : values) {
                    builder.append(value.toString()).append(",");
                }
                builder.deleteCharAt(builder.length() - 1); // Remove trailing comma
                outputValue.set(builder.toString());
                context.write(key, outputValue);
            } catch (Exception e) {
                Logger logger = Logger.getLogger(IndexInverterReducer.class);
                logger.error("Error reducing key: " + key.toString(), e);
            }
        }
    }    
    
    /**
     * Configures and executes the MapReduce job.
     *
     * @param args Command-line arguments: input path and output path.
     * @return 0 if the job completes successfully, 1 otherwise.
     * @throws Exception If an error occurs during job execution.
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = null;
        try {
            Configuration conf = super.getConf();
            job = Job.getInstance(conf, "IndexInverterJob");
            job.setJarByClass(IndexInverterJob.class);

            Path in = new Path(args[0]);
            Path out = new Path(args[1]);
            out.getFileSystem(conf).delete(out, true); // Clean up output path
            FileInputFormat.setInputPaths(job, in);
            FileOutputFormat.setOutputPath(job, out);
            
            job.setMapperClass(IndexInverterMapper.class);
            job.setReducerClass(IndexInverterReducer.class);
            
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } catch (Exception e) {
            Logger logger = Logger.getLogger(IndexInverterJob.class);
            logger.error("Error setting up the job", e);
            throw e;
        }
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Main method to execute the job.
     *
     * @param args Command-line arguments.
     */
    public static void main(String[] args) {
        int result = 1;
        try {
            result = ToolRunner.run(new Configuration(),  
                    new IndexInverterJob(), args);
        } catch (Exception e) {
            Logger logger = Logger.getLogger(IndexInverterJob.class);
            logger.error("Error running the job", e);
        } finally {
            System.exit(result);
        }
    }
}

