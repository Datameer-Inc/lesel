package datameer.lesel.s3vm;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datameer.lesel.core.Instances;

public class S3vmTest {

	@Rule
	public TemporaryFolder _testFolder = new TemporaryFolder();

	@Test
	public void testRunLocalS3vm() {
		Instances instances = Instances.loadFromFile("src/data/twoGaussians.csv");

		LocalS3vm s3vm = new LocalS3vm(instances);

		s3vm.run(1.0, 10.0);

	}

	@Test
	public void testRunS3vm() throws Exception {
		File inputFile = new File("src/data/twoGaussians.csv");
		File testFile = _testFolder.newFile("inputCsv.txt");
		FileUtils.copyFile(inputFile, testFile);

		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///");

		S3vm s3vm = new S3vm();
		s3vm.setConf(conf);

		int result = s3vm.run(new String[] { testFile.getPath() });

		assertThat(result).isEqualTo(0);
	}

}
