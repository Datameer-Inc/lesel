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

import datameer.lesel.core.MapperTestBase;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class NormalizeMapperTest extends MapperTestBase<LongWritable, Text, NullWritable, InstanceWritable> {
	private FSDataOutputStream _outputStream;

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();

		_outputStream = mock(FSDataOutputStream.class);
		when(_testFileSystem.create(any(Path.class))).thenReturn(_outputStream);
	}

	@Test
	public void testNormalizeMapper() throws IOException, InterruptedException {
		InstanceMeans means = InstanceMeans
				.createFromJson("{\"_unlabeledInstancesMeans\":[2.0,-2.0,5.0],\"_labelsMean\":0.0,\"_labeledCount\":2,\"_unlabeledCount\":2}");
		NormalizeMapper mapper = new NormalizeMapper(means);
		List<InstanceWritable> outputValues = Lists.newArrayList();
		Context context = createContext(mapper, outputValues);

		mapper.map(new LongWritable(3), new Text("1.0, 2.0, 3.0, 0.0"), context);
		mapper.map(new LongWritable(3), new Text("3.0, 2.0, 1.0, 0.0"), context);
		mapper.map(new LongWritable(3), new Text("4.0, 5.0, -1.0, 0.0"), context);
		mapper.map(new LongWritable(3), new Text("-1.0, 5.0, 4.0, 0.0"), context);

		assertThat(outputValues).hasSize(4);
		assertThat(outputValues.get(0).getInstance().getX()).isEqualTo(new double[] { 0.0, 3.0, 0.5 }, Delta.delta(1e-6));
		assertThat(outputValues.get(1).getInstance().getX()).isEqualTo(new double[] { 2.0, 3.0, -1.5 }, Delta.delta(1e-6));
		assertThat(outputValues.get(2).getInstance().getX()).isEqualTo(new double[] { 3.0, 6.0, -3.5 }, Delta.delta(1e-6));
		assertThat(outputValues.get(3).getInstance().getX()).isEqualTo(new double[] { -2.0, 6.0, 1.5 }, Delta.delta(1e-6));
	}
}