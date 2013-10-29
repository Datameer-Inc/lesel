package datameer.lesel.generative;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.fest.assertions.Delta;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import datameer.lesel.core.Instance;
import datameer.lesel.core.MapperTestBase;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MaximizationMapperTest extends MapperTestBase<NullWritable, InstanceWithWeightsWritable, NullWritable, Text> {
	private static final Gson GSON = new Gson();

	private FSDataOutputStream _statisticsOutputStream;

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();

		_statisticsOutputStream = mock(FSDataOutputStream.class);
		when(_testFileSystem.create(any(Path.class))).thenReturn(_statisticsOutputStream);
	}

	@Test
	public void testMapperNoTargetLabel() throws IOException, InterruptedException {
		MaximizationMapper mapper = new MaximizationMapper();
		List<Text> outputValues = Lists.newArrayList();
		Context context = createContext(mapper, outputValues);

		InstanceWithWeightsWritable instanceWithWeights = new InstanceWithWeightsWritable(Instance.createFromText("1.0, 2.0, 3.0, 0.0"), new double[] { 0.3,
				0.7 });
		mapper.map(NullWritable.get(), instanceWithWeights, context);

		instanceWithWeights = new InstanceWithWeightsWritable(Instance.createFromText("3.0, 2.0, 1.0, 0.0"), new double[] { 0.7, 0.3 });
		mapper.map(NullWritable.get(), instanceWithWeights, context);

		instanceWithWeights = new InstanceWithWeightsWritable(Instance.createFromText("4.0, 5.0, -1.0, 0.0"), new double[] { 0.2, 0.8 });
		mapper.map(NullWritable.get(), instanceWithWeights, context);

		instanceWithWeights = new InstanceWithWeightsWritable(Instance.createFromText("-1.0, 5.0, 4.0, 0.0"), new double[] { 0.8, 0.2 });
		mapper.map(NullWritable.get(), instanceWithWeights, context);

		mapper.cleanup(context);

		assertThat(outputValues).hasSize(1);
		String statisticsJson = outputValues.get(0).toString();
		MomentStatistics[] statistics = GSON.fromJson(statisticsJson, MomentStatistics[].class);

		assertThat(statistics[0]._means).isEqualTo(new double[] { 1.2, 3.5, 2.3 }, Delta.delta(1e-6));
		assertThat(statistics[1]._means).isEqualTo(new double[] { 2.3, 3.5, 1.2 }, Delta.delta(1e-6));
		assertThat(statistics[0]._m2).isEqualTo(new double[] { 7.72, 4.5, 5.82 }, Delta.delta(1e-6));
		assertThat(statistics[1]._m2).isEqualTo(new double[] { 5.82, 4.5, 7.72 }, Delta.delta(1e-6));
	}

}
