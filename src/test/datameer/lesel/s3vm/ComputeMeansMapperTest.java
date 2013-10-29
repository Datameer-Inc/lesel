package datameer.lesel.s3vm;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.fest.assertions.Delta;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import datameer.lesel.core.MapperTestBase;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ComputeMeansMapperTest extends MapperTestBase<LongWritable, Text, NullWritable, Text> {
	private static final Gson GSON = new Gson();

	private FSDataOutputStream _statisticsOutputStream;

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();

		_statisticsOutputStream = mock(FSDataOutputStream.class);
		when(_testFileSystem.create(any(Path.class))).thenReturn(_statisticsOutputStream);
	}

	@Test
	public void testMapper() throws IOException, InterruptedException {
		ComputeMeansMapper mapper = new ComputeMeansMapper();
		List<Text> outputValues = Lists.newArrayList();
		Context context = createContext(mapper, outputValues);

		Text instance = new Text("1.0, 2.0, 3.0, 0.0");
		mapper.map(new LongWritable(1L), instance, context);

		instance = new Text("3.0, 2.0, 1.0, 1.0");
		mapper.map(new LongWritable(1L), instance, context);

		instance = new Text("4.0, 5.0, -1.0, -1.0");
		mapper.map(new LongWritable(1L), instance, context);

		instance = new Text("-1.0, 5.0, 4.0, 0.0");
		mapper.map(new LongWritable(1L), instance, context);

		mapper.cleanup(context);

		assertThat(outputValues).hasSize(1);
		String meansJson = outputValues.get(0).toString();
		InstanceMeans means = GSON.fromJson(meansJson, InstanceMeans.class);

		assertThat(means.getUnlabeledInstancesMeans()).isEqualTo(new double[] { 0.0, 3.5, 3.5 }, Delta.delta(1e-6));
		assertThat(means.getLabelsMean()).isEqualTo(0.0, Delta.delta(1e-6));
		assertThat(means.getNumberOfInstances()).isEqualTo(4);
	}

}
