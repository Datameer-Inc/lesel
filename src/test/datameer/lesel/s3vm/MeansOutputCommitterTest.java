package datameer.lesel.s3vm;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.fest.assertions.Delta;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.gson.Gson;

public class MeansOutputCommitterTest {
	private static final Gson GSON = new Gson();

	@Rule
	public TemporaryFolder _tempFolder = new TemporaryFolder();

	@Test
	public void testMeansOutputCommitter() throws IOException {
		Configuration configuration = new Configuration();
		TaskAttemptContext taskContext = new TaskAttemptContext(configuration, new TaskAttemptID());
		Path outputPath = new Path(_tempFolder.getRoot().getAbsolutePath());
		MeansOutputCommitter committer = new MeansOutputCommitter(outputPath, taskContext);

		File statisticsFolder = writeMeansFiles(outputPath);

		JobContext jobContext = new JobContext(configuration, new JobID());
		committer.commitJob(jobContext);

		String overallMeansJson = FileUtils.readFileToString(new File(statisticsFolder, MeansOutputCommitter.OVERALL_MEANS_FILENAME));
		assertThat(overallMeansJson).isNotNull();

		InstanceMeans means = GSON.fromJson(overallMeansJson, InstanceMeans.class);
		assertThat(means.getNumberOfInstances()).isEqualTo(20);
		assertThat(means.getLabelsMean()).isEqualTo(0.5, Delta.delta(1e-6));
		assertThat(means.getUnlabeledInstancesMeans()).isEqualTo(new double[] { -0.25, 1.0, 1.5 }, Delta.delta(1e-6));
	}

	private File writeMeansFiles(Path outputPath) throws IOException {
		File meansFolder = new File(outputPath.toUri().getPath());
		meansFolder.mkdirs();
		writeFile(meansFolder, "part-m-00000", "{\"_unlabeledInstancesMeans\":[1.0,3.0,7.0],\"_labelsMean\":3.0,\"_labeledCount\":3,\"_unlabeledCount\":3}");
		writeFile(meansFolder, "part-m-00001", "{\"_unlabeledInstancesMeans\":[-5.0,8.0,2.0],\"_labelsMean\":-1.0,\"_labeledCount\":5,\"_unlabeledCount\":2}");
		writeFile(meansFolder, "part-m-00002", "{\"_unlabeledInstancesMeans\":[2.0,-3.0,3.0],\"_labelsMean\":4.0,\"_labeledCount\":4,\"_unlabeledCount\":3}");
		return meansFolder;
	}

	private void writeFile(File sumsFolder, String filename, String fileContent) throws IOException {
		File sumsFile = new File(sumsFolder, filename);

		FileUtils.writeStringToFile(sumsFile, fileContent);
	}
}
