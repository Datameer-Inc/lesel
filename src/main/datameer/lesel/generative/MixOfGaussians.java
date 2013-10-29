package datameer.lesel.generative;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class MixOfGaussians extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(MixOfGaussians.class);

	private static final int DEFAULT_NUMBER_OF_ITERATIONS = 10;

	private static final String INITIAL_EXPECTATION_JOB_NAME = "expectation-0";

	public static int NUMBER_OF_COMPONENTS = 2;

	private String _inputPath;
	private Path _workPath;
	private int _numberOfIterations = DEFAULT_NUMBER_OF_ITERATIONS;

	public MixOfGaussians() {
	}

	private String runEmIteration() throws IOException, InterruptedException, ClassNotFoundException {
		String currentFilename = null;
		for (int i = 0; i < _numberOfIterations; i++) {
			if (i == 0) {
				runCreateInitialWeightsJob();
				currentFilename = INITIAL_EXPECTATION_JOB_NAME;
			} else {
				currentFilename = runExpectationJob(i, currentFilename);
			}

			currentFilename = runMaximizationJob(i, currentFilename);
		}

		return currentFilename;
	}

	private String runExpectationJob(int iteration, String inputFilename) throws IOException, InterruptedException, ClassNotFoundException {
		String jobName = "expectation-" + Integer.toString(iteration);
		Job job = new Job(new Configuration(getConf()), jobName);

		job.setJarByClass(MixOfGaussians.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(InstanceWithWeightsWritable.class);

		job.setMapperClass(ExpectationMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		String statisticsPath = new Path(new Path(_workPath, inputFilename), MomentStatisticsOutputCommitter.OVERALL_STATISTICS_FILENAME).toUri().getPath();
		job.getConfiguration().set(ExpectationMapper.STATISTICS_PATH_CONFIG, statisticsPath);

		Path inputPath = new Path(_workPath, INITIAL_EXPECTATION_JOB_NAME);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(_workPath, jobName));

		LOG.info("Starting job " + jobName);
		job.waitForCompletion(true);

		return jobName;
	}

	private String runMaximizationJob(int iteration, String inputFilename) throws IOException, InterruptedException, ClassNotFoundException {
		String jobName = "maximization-" + Integer.toString(iteration);
		Job job = new Job(getConf(), jobName);

		job.setJarByClass(MixOfGaussians.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MaximizationMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(MomentStatisticsOutputFormat.class);

		Path inputPath = new Path(_workPath, inputFilename);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(_workPath, jobName));

		LOG.info("Starting job " + jobName);
		job.waitForCompletion(true);

		// Work around a Hadoop bug
		// (https://issues.apache.org/jira/browse/MAPREDUCE-2350)
		if (getConf().get("mapred.job.tracker").equals("local")) {
			MomentStatisticsOutputCommitter committer = new MomentStatisticsOutputCommitter(new Path(_workPath, jobName), new TaskAttemptContext(getConf(),
					new TaskAttemptID()));
			committer.mergeStatistics(getConf());
		}

		return jobName;
	}

	private void runCreateInitialWeightsJob() throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(getConf(), INITIAL_EXPECTATION_JOB_NAME);

		job.setJarByClass(MixOfGaussians.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(InstanceWithWeightsWritable.class);

		job.setMapperClass(CreateInitialWeightsMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		Path inputPath = new Path(_inputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(_workPath, INITIAL_EXPECTATION_JOB_NAME));

		LOG.info("Starting job " + INITIAL_EXPECTATION_JOB_NAME);
		job.waitForCompletion(true);
	}

	@Override
	public int run(String[] args) throws Exception {
		_inputPath = args[0];
		_workPath = new Path(_inputPath).getParent();

		String resultFilename = runEmIteration();

		Path statisticsPath = new Path(new Path(_workPath, resultFilename), MomentStatisticsOutputCommitter.OVERALL_STATISTICS_FILENAME);
		MomentStatistics[] statistics = MomentStatistics.readStatisticsFile(statisticsPath, statisticsPath.getFileSystem(getConf()));
		MixtureModel model = MixtureModel.createFromStatistics(statistics);

		System.out.println();
		System.out.print(model.toString());

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MixOfGaussians(), args);
		System.exit(res);
	}
}
