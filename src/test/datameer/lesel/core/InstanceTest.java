package datameer.lesel.core;

import static org.fest.assertions.Assertions.assertThat;

import org.apache.hadoop.io.ArrayWritable;
import org.junit.Test;

public class InstanceTest {
	@Test
	public void testConvertToArray() {
		Instance instance = Instance.create(new double[] { 1.0, 2.0, 3.0, 4.0 }, 0.0);

		ArrayWritable array = instance.getAsArray();
		Instance newInstance = Instance.createFromArray(array);

		assertThat(newInstance.getX()).isEqualTo(instance.getX());
		assertThat(newInstance.getY()).isEqualTo(instance.getY());
	}

	@Test
	public void testCreateFromText() {
		Instance instance = Instance.create(new double[] { 1.0, 2.0, 3.0, 4.0 }, 0.0);

		String text = instance.toString();
		Instance newInstance = Instance.createFromText(text);

		assertThat(newInstance.getX()).isEqualTo(instance.getX());
		assertThat(newInstance.getY()).isEqualTo(instance.getY());
	}
}
