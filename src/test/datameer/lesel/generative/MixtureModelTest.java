package datameer.lesel.generative;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

public class MixtureModelTest {
	@Test
	public void testConvertToJson() {
		MixtureModel model = MixtureModel.create(new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 } },
				new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 } }, new double[] { 0.5, 0.5 });
		String json = model.getAsJson();
		MixtureModel newModel = MixtureModel.createFromJson(json);

		assertThat(newModel.getMu()).isEqualTo(model.getMu());
		assertThat(newModel.getSigma()).isEqualTo(model.getSigma());
		assertThat(newModel.getTau()).isEqualTo(model.getTau());

	}
}
