package datameer.lesel.generative;

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

public class MomentStatisticsOutputCommitterTest {
	private static final Gson GSON = new Gson();

	@Rule
	public TemporaryFolder _tempFolder = new TemporaryFolder();

	@Test
	public void testWeightsWithSumsOutputCommitter() throws IOException {
		Configuration configuration = new Configuration();
		TaskAttemptContext taskContext = new TaskAttemptContext(configuration, new TaskAttemptID());
		Path outputPath = new Path(_tempFolder.getRoot().getAbsolutePath());
		MomentStatisticsOutputCommitter committer = new MomentStatisticsOutputCommitter(outputPath, taskContext);

		File statisticsFolder = writeStatisticsFiles(outputPath);

		JobContext jobContext = new JobContext(configuration, new JobID());
		committer.commitJob(jobContext);

		String overallStatisticsJson = FileUtils.readFileToString(new File(statisticsFolder, MomentStatisticsOutputCommitter.OVERALL_STATISTICS_FILENAME));
		assertThat(overallStatisticsJson).isNotNull();

		MomentStatistics[] statistics = GSON.fromJson(overallStatisticsJson, MomentStatistics[].class);
		assertThat(statistics[0]._weightSum).isEqualTo(3.1, Delta.delta(1e-6));
		assertThat(statistics[1]._weightSum).isEqualTo(2.9, Delta.delta(1e-6));

		assertThat(statistics[0]._means).isEqualTo(new double[] { 2.516129, 3.064516 }, Delta.delta(1e-5));
		assertThat(statistics[1]._means).isEqualTo(new double[] { 2.827586, 2.586207 }, Delta.delta(1e-5));

		assertThat(statistics[0]._m2).isEqualTo(new double[] { 32.374194, 2.187097 }, Delta.delta(1e-5));
		assertThat(statistics[1]._m2).isEqualTo(new double[] { 28.813793, 2.303448 }, Delta.delta(1e-5));
	}

	private File writeStatisticsFiles(Path outputPath) throws IOException {
		File statisticsFolder = new File(outputPath.toUri().getPath());
		statisticsFolder.mkdirs();
		writeFile(
				statisticsFolder,
				"part-m-00000",
				"[{\"_n\":2,\"_weightSum\":0.9,\"_means\":[2.333333333333,3.333333333333],\"_m2\":[0.8,0.8]},{\"_n\":2,\"_weightSum\":1.1,\"_means\":[1.727272727273,2.727272727273],\"_m2\":[1.018181818182,1.018181818182]}]");
		writeFile(
				statisticsFolder,
				"part-m-00001",
				"[{\"_n\":2,\"_weightSum\":1.4,\"_means\":[-0.071428571429,2.642857142857],\"_m2\":[2.892857142857,0.321428571429]},{\"_n\":2,\"_weightSum\":0.6,\"_means\":[-1.5,2.166666666667],\"_m2\":[0.75,0.083333333333 ]}]");
		writeFile(
				statisticsFolder,
				"part-m-00002",
				"[{\"_n\":2,\"_weightSum\":0.8,\"_means\":[7.25,3.5],\"_m2\":[1.35,0.6]},{\"_n\":2,\"_weightSum\":1.2,\"_means\":[6.0,2.666666666667],\"_m2\":[2.4,1.066666666667]}]");
		return statisticsFolder;
	}

	private void writeFile(File sumsFolder, String filename, String fileContent) throws IOException {
		File sumsFile = new File(sumsFolder, filename);

		FileUtils.writeStringToFile(sumsFile, fileContent);
	}
}
