package datameer.lesel.generative;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import datameer.lesel.core.Instance;
import datameer.lesel.core.MapperTestBase;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ExpectationMapperTest extends MapperTestBase<NullWritable, InstanceWithWeightsWritable, NullWritable, InstanceWithWeightsWritable> {

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();
	}

	@Test
	public void testMapperNoTargetLabel() throws IOException, InterruptedException {
		MixtureModel model = MixtureModel.create(new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 } },
				new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 } }, new double[] { 0.5, 0.5 });

		ExpectationMapper mapper = new ExpectationMapper(model);
		List<InstanceWithWeightsWritable> outputValues = Lists.newArrayList();
		Context context = createContext(mapper, outputValues);

		Instance instance = Instance.createFromText("1.0, 2.0, 3.0, 0.0");
		mapper.map(NullWritable.get(), new InstanceWithWeightsWritable(instance, new double[] { 0.0, 0.0 }), context);

		assertThat(outputValues).hasSize(1);

		Instance outputInstance = outputValues.get(0).getInstance();
		assertThat(outputInstance.getX()).isEqualTo(instance.getX());
		assertThat(outputInstance.getY()).isEqualTo(instance.getY());

		double[] weights = outputValues.get(0).getWeights();
		assertThat(weights[0]).isGreaterThan(0.98);
		assertThat(weights[1]).isLessThan(0.02);
	}

	@Test
	public void testMapperExistingTargetLabel() throws IOException, InterruptedException {
		MixtureModel model = MixtureModel.create(new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 } },
				new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 } }, new double[] { 0.5, 0.5 });

		ExpectationMapper mapper = new ExpectationMapper(model);
		List<InstanceWithWeightsWritable> outputValues = Lists.newArrayList();
		Context context = createContext(mapper, outputValues);

		Instance instance = Instance.createFromText("1.0, 2.0, 3.0, 1.0");
		mapper.map(NullWritable.get(), new InstanceWithWeightsWritable(instance, new double[] { 0.0, 0.0 }), context);

		assertThat(outputValues).hasSize(1);

		Instance outputInstance = outputValues.get(0).getInstance();
		assertThat(outputInstance.getX()).isEqualTo(instance.getX());
		assertThat(outputInstance.getY()).isEqualTo(instance.getY());

		double[] weights = outputValues.get(0).getWeights();

		assertThat(weights[0]).isEqualTo(1.0);
		assertThat(weights[1]).isEqualTo(0.0);
	}

}
