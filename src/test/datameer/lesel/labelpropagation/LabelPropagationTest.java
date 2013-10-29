package datameer.lesel.labelpropagation;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datameer.lesel.core.Instances;

public class LabelPropagationTest {

	@Rule
	public TemporaryFolder _testFolder = new TemporaryFolder();

	@Test
	public void testRunLocalLabelPropagation() {
		Instances instances = Instances.loadFromFile("src/data/twoGaussians.csv");

		LocalLabelPropagation labelPropagation = new LocalLabelPropagation(instances);

		labelPropagation.run();

	}

	@Test
	public void testRunLabelPropagation() throws Exception {
		File inputFile = new File("src/data/twoGaussians.csv");
		File testFile = _testFolder.newFile("inputCsv.txt");
		FileUtils.copyFile(inputFile, testFile);

		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///");

		LabelPropagation labelPropagation = new LabelPropagation();
		labelPropagation.setConf(conf);

		int result = labelPropagation.run(new String[] { testFile.getPath() });

		assertThat(result).isEqualTo(0);
	}

}