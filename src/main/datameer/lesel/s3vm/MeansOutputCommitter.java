package datameer.lesel.s3vm;

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

public class MeansOutputCommitter extends FileOutputCommitter {
	private static final Logger LOG = Logger.getLogger(MeansOutputCommitter.class);
	private static final String PARTS_FILENAME_PREFIX = "part";
	public static final String OVERALL_MEANS_FILENAME = "overallMeans";

	private static final PathFilter PARTS_FILTER = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return path.getName().startsWith(PARTS_FILENAME_PREFIX);
		}

	};

	private Path _outputPath = null;

	public MeansOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
		super(outputPath, context);
		_outputPath = outputPath;
	}

	public void commitJob(JobContext context) throws IOException {
		super.commitJob(context);

		mergeMeans(context.getConfiguration());
	}

	public void mergeMeans(Configuration config) throws IOException {
		FileSystem fileSystem = _outputPath.getFileSystem(config);

		if (!fileSystem.exists(_outputPath)) {
			LOG.error("Cannot find moment statistics path at: " + _outputPath);
			throw new IllegalStateException();
		}

		InstanceMeans overallMeans = null;

		FileStatus[] meansFiles = fileSystem.listStatus(_outputPath, PARTS_FILTER);
		for (FileStatus meansFile : meansFiles) {
			InstanceMeans means = InstanceMeans.readMeansFile(meansFile.getPath(), fileSystem);

			if (overallMeans == null) {
				overallMeans = means;
			} else {
				overallMeans.merge(means);
			}
		}

		Path overallMeansPath = new Path(_outputPath, OVERALL_MEANS_FILENAME);
		InstanceMeans.writeMeansFile(overallMeansPath, fileSystem, overallMeans);
		LOG.info("Merged " + meansFiles.length + " means files to " + overallMeansPath);
	}

}