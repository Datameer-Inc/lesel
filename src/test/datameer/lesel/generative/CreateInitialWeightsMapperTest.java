package datameer.lesel.generative;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import datameer.lesel.core.MapperTestBase;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class CreateInitialWeightsMapperTest extends MapperTestBase<LongWritable, Text, NullWritable, InstanceWithWeightsWritable> {

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();
	}

	@Test
	public void testImportCsvMapper() throws IOException, InterruptedException {
		CreateInitialWeightsMapper mapper = new CreateInitialWeightsMapper();

		List<InstanceWithWeightsWritable> outputValues = Lists.newArrayList();
		Context context = createContext(mapper, outputValues);

		mapper.map(new LongWritable(10), new Text("1.0,  2.0, 3.0, 0.0"), context);

		assertThat(outputValues).hasSize(1);
		assertThat(outputValues.get(0).getInstance().getX()).isEqualTo(new double[] { 1.0, 2.0, 3.0 });
		assertThat(outputValues.get(0).getInstance().getY()).isEqualTo(0.0);
	}
}
