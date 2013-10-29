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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.fest.assertions.Delta;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import datameer.lesel.core.Instance;
import datameer.lesel.core.MapperTestBase;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SgdMapperTest extends MapperTestBase<NullWritable, InstanceWritable, DoubleWritable, InstanceWritable> {
	private FSDataOutputStream _outputStream;

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();

		_outputStream = mock(FSDataOutputStream.class);
		when(_testFileSystem.create(any(Path.class))).thenReturn(_outputStream);
	}

	@Test
	public void testSgdMapper() throws IOException, InterruptedException {
		SgdMapper mapper = new SgdMapper();
		List<InstanceWritable> outputValues = Lists.newArrayList();
		Context context = createContext(mapper, outputValues);

		mapper.map(NullWritable.get(), new InstanceWritable(Instance.createFromText("1.0, 2.0, 3.0, 0.0")), context);
		assertThat(outputValues).hasSize(1);
		assertThat(outputValues.get(0).getInstance().getX()).isEqualTo(new double[] { 1.0, 2.0, 3.0 }, Delta.delta(1e-6));
	}
}