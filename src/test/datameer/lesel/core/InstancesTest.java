package datameer.lesel.core;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

public class InstancesTest {

	@Test
	public void testLoadExampleFile() {
		Instances instances = Instances.loadFromFile("src/data/instancesTestData.csv");
		assertThat(instances.getSize()).isEqualTo(3);
		assertThat(instances.getNumberOfAttributes()).isEqualTo(4);
		assertThat(instances.getInstance(1).getX()).containsOnly(5.0, 6.0, 7.0, 8.0);
	}

}
