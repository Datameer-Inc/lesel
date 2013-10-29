package datameer.lesel.generative;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

public class MomentStatisticsOutputCommitter extends FileOutputCommitter {
	private static final Logger LOG = Logger.getLogger(MomentStatisticsOutputCommitter.class);
	private static final String PARTS_FILENAME_PREFIX = "part";
	public static final String OVERALL_STATISTICS_FILENAME = "overallStatistics";

	private static final PathFilter PARTS_FILTER = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return path.getName().startsWith(PARTS_FILENAME_PREFIX);
		}

	};

	private Path _outputPath = null;

	public MomentStatisticsOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
		super(outputPath, context);
		_outputPath = outputPath;
	}

	public void commitJob(JobContext context) throws IOException {
		super.commitJob(context);

		mergeStatistics(context.getConfiguration());
	}

	public void mergeStatistics(Configuration config) throws IOException {
		FileSystem fileSystem = _outputPath.getFileSystem(config);

		if (!fileSystem.exists(_outputPath)) {
			LOG.error("Cannot find moment statistics path at: " + _outputPath);
			throw new IllegalStateException();
		}

		MomentStatistics[] overallStatistics = new MomentStatistics[MixOfGaussians.NUMBER_OF_COMPONENTS];

		FileStatus[] statisticsFiles = fileSystem.listStatus(_outputPath, PARTS_FILTER);
		for (FileStatus statisticsFile : statisticsFiles) {
			MomentStatistics[] statistics = MomentStatistics.readStatisticsFile(statisticsFile.getPath(), fileSystem);

			for (int i = 0; i < statistics.length; i++) {
				if (overallStatistics[i] == null) {
					overallStatistics[i] = statistics[i];
				} else {
					overallStatistics[i].merge(statistics[i]);
				}
			}
		}

		Path overallStatisticsPath = new Path(_outputPath, OVERALL_STATISTICS_FILENAME);
		MomentStatistics.writeStatisticsFile(overallStatisticsPath, fileSystem, overallStatistics);
		LOG.info("Merged " + statisticsFiles.length + " statistics files to " + overallStatisticsPath);
	}

}
