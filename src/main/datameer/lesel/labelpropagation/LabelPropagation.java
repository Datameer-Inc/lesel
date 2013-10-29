package datameer.lesel.labelpropagation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import datameer.lesel.s3vm.S3vm;

public class LabelPropagation extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(LabelPropagation.class);

	private static final String COMPUTE_BLOCK_NEIGHBORS_NAME = "BlockNeighbors";
	private static final String COMPUTE_NEIGHBORS_NAME = "Neighbors";
	private static final String LABEL_PROPAGATION_JOB_NAME = "LabelPropagation";
	private static final String CONVERT_TO_TEXT_JOB_NAME = "ConvertToText";

	private static final int NUMBER_OF_ITERATIONS = 15;

	private String _inputPath;
	private Path _workPath;

	public LabelPropagation() {
	}

	private void runConvertToTextJob() throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(getConf(), CONVERT_TO_TEXT_JOB_NAME);

		job.setJarByClass(LabelPropagation.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ConvertToTextMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path inputPath = new Path(_workPath, LABEL_PROPAGATION_JOB_NAME + NUMBER_OF_ITERATIONS);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(_workPath, CONVERT_TO_TEXT_JOB_NAME));

		LOG.info("Starting job " + CONVERT_TO_TEXT_JOB_NAME);
		job.waitForCompletion(true);
	}

	private void runLabelPropagationIteration(int iteration) throws IOException, InterruptedException, ClassNotFoundException {
		String jobName = LABEL_PROPAGATION_JOB_NAME + iteration;

		Job job = new Job(getConf(), jobName);

		job.setJarByClass(LabelPropagation.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LabelPropagationWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(InstanceWithNeighborsWritable.class);

		job.setMapperClass(LabelPropagationMapper.class);
		job.setReducerClass(LabelPropagationReducer.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		Path inputPath = new Path(_workPath, iteration == 0 ? COMPUTE_NEIGHBORS_NAME : LABEL_PROPAGATION_JOB_NAME + (iteration - 1));
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(_workPath, jobName));

		LOG.info("Starting job " + jobName);
		job.waitForCompletion(true);
	}

	private void runComputeNeighborsJob() throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(getConf(), COMPUTE_NEIGHBORS_NAME);

		job.setJarByClass(LabelPropagation.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(InstanceWithNeighborsWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(InstanceWithNeighborsWritable.class);

		job.setMapperClass(NearestNeighborsMapper.class);
		job.setReducerClass(NearestNeighborsReducer.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		Path inputPath = new Path(_workPath, COMPUTE_BLOCK_NEIGHBORS_NAME);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(_workPath, COMPUTE_NEIGHBORS_NAME));

		LOG.info("Starting job " + COMPUTE_NEIGHBORS_NAME);
		job.waitForCompletion(true);
	}

	private void runComputeBlockNeighborsJob() throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(getConf(), COMPUTE_BLOCK_NEIGHBORS_NAME);

		job.setJarByClass(LabelPropagation.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BlockInstanceWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(InstanceWithNeighborsWritable.class);

		job.setMapperClass(BlockNeighborsMapper.class);
		job.setReducerClass(BlockNeighborsReducer.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		Path inputPath = new Path(_inputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(_workPath, COMPUTE_BLOCK_NEIGHBORS_NAME));

		LOG.info("Starting job " + COMPUTE_BLOCK_NEIGHBORS_NAME);
		job.waitForCompletion(true);
	}

	@Override
	public int run(String[] args) throws Exception {
		_inputPath = args[0];
		_workPath = new Path(_inputPath).getParent();

		runComputeBlockNeighborsJob();
		runComputeNeighborsJob();

		for (int iteration = 0; iteration <= NUMBER_OF_ITERATIONS; iteration++) {
			runLabelPropagationIteration(iteration);
		}

		runConvertToTextJob();

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new S3vm(), args);
		System.exit(res);
	}
}