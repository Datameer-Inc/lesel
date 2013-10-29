package datameer.lesel.generative;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datameer.lesel.core.Instances;

public class MixOfGaussiansTest {
	@Rule
	public TemporaryFolder _testFolder = new TemporaryFolder();

	@Test
	public void testRunLocalMixOfGaussians() {
		Instances instances = Instances.loadFromFile("src/data/twoGaussians.csv");

		LocalMixOfGaussians mixOfGaussians = new LocalMixOfGaussians(instances);

		mixOfGaussians.execute();

		double[][] mu = mixOfGaussians.getMu();
		double[][] sigma = mixOfGaussians.getSigma();

		for (int i = 0; i < 4; i++) {
			assertThat(Math.abs(mu[0][i] + 5.0)).isLessThan(0.5);
			assertThat(Math.abs(mu[1][i] - 5.0)).isLessThan(0.5);

			assertThat(Math.abs(Math.sqrt(sigma[0][i]) - i - 1)).isLessThan(0.5);
			assertThat(Math.abs(Math.sqrt(sigma[1][i]) - (4 - i))).isLessThan(0.5);
		}

	}

	@Test
	public void testRunMixOfGaussians() throws Exception {
		File inputFile = new File("src/data/twoGaussians.csv");
		File testFile = _testFolder.newFile("inputCsv.txt");
		FileUtils.copyFile(inputFile, testFile);

		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///");

		MixOfGaussians mixOfGaussians = new MixOfGaussians();
		mixOfGaussians.setConf(conf);

		int result = mixOfGaussians.run(new String[] { testFile.getPath() });

		assertThat(result).isEqualTo(0);
	}

}
