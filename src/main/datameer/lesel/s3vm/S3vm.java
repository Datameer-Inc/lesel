package datameer.lesel.s3vm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class S3vm extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(S3vm.class);

	private static final String COMPUTE_MEANS_JOB_NAME = "Means";
	private static final String NORMALIZE_DATA_JOB_NAME = "NormalizedData";
	private static final String SGD_JOB_NAME = "SGD";

	private String _inputPath;
	private Path _workPath;

	public S3vm() {
	}

	private void runSgdJob() throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(new Configuration(getConf()), SGD_JOB_NAME);

		job.setJarByClass(S3vm.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(InstanceWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SgdMapper.class);
		job.setReducerClass(SgdReducer.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().setFloat(SgdReducer.LEARNING_RATE, 10.0F);
		job.getConfiguration().setFloat(SgdReducer.REGULARIZER_CONSTANT, 1.0F);
		job.getConfiguration().setFloat(SgdReducer.UNLABELED_WEIGHT, 0.5F);

		double bias = readBiasFromMeansFile();
		job.getConfiguration().setFloat(SgdReducer.BIAS, (float) bias);

		Path inputPath = new Path(_workPath, NORMALIZE_DATA_JOB_NAME);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(_workPath, SGD_JOB_NAME));

		LOG.info("Starting job " + SGD_JOB_NAME);
		job.waitForCompletion(true);
	}

	private double readBiasFromMeansFile() throws IOException {
		Path meansPath = new Path(_workPath, COMPUTE_MEANS_JOB_NAME + "/" + MeansOutputCommitter.OVERALL_MEANS_FILENAME);
		FileSystem fileSystem = meansPath.getFileSystem(getConf());
		InstanceMeans means = InstanceMeans.readMeansFile(meansPath, fileSystem);
		return means.getLabelsMean();
	}

	private void runNormalizeInstancesJob() throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(getConf(), NORMALIZE_DATA_JOB_NAME);

		job.setJarByClass(S3vm.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(InstanceWritable.class);

		job.setMapperClass(NormalizeMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.getConfiguration().set(NormalizeMapper.MEANS_PATH, new Path(_workPath, COMPUTE_MEANS_JOB_NAME).toUri().toASCIIString());

		FileInputFormat.addInputPath(job, new Path(_inputPath));
		FileOutputFormat.setOutputPath(job, new Path(_workPath, NORMALIZE_DATA_JOB_NAME));

		LOG.info("Starting job " + NORMALIZE_DATA_JOB_NAME);
		job.waitForCompletion(true);
	}

	private void runComputeMeansJob() throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(getConf(), COMPUTE_MEANS_JOB_NAME);

		job.setJarByClass(S3vm.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ComputeMeansMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path inputPath = new Path(_inputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(_workPath, COMPUTE_MEANS_JOB_NAME));

		LOG.info("Starting job " + COMPUTE_MEANS_JOB_NAME);
		job.waitForCompletion(true);

		// Work around a Hadoop bug
		// (https://issues.apache.org/jira/browse/MAPREDUCE-2350)
		if (getConf().get("mapred.job.tracker").equals("local")) {
			MeansOutputCommitter committer = new MeansOutputCommitter(new Path(_workPath, COMPUTE_MEANS_JOB_NAME), new TaskAttemptContext(getConf(),
					new TaskAttemptID()));
			committer.mergeMeans(getConf());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		_inputPath = args[0];
		_workPath = new Path(_inputPath).getParent();

		runComputeMeansJob();
		runNormalizeInstancesJob();
		runSgdJob();

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new S3vm(), args);
		System.exit(res);
	}
}
