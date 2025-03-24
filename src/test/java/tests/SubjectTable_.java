package tests;

import io.intino.alexandria.SubjectTable;
import io.intino.alexandria.SubjectStore;
import io.intino.alexandria.model.table.Column;
import io.intino.alexandria.model.table.Format;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static io.intino.alexandria.model.table.Column.Numerical.Normalization.MinMax;
import static io.intino.alexandria.model.table.operators.CategoricalOperator.Mode;
import static io.intino.alexandria.model.table.operators.NumericalFunction.*;
import static io.intino.alexandria.model.table.operators.TemporalFunction.*;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;

public class SubjectTable_ {
	private final static Instant from = Instant.parse("2025-01-01T00:00:00Z");
	private final static Instant to = Instant.parse("2025-02-01T00:00:00Z");
	private final static String expected = """
		1	1	0.0	0						3
		8	1	22.0	44	19	25	19	25		3
		15	1	0.0	0						3
		22	1	0.0	0						3
		29	1	14.0	14	14	14	14	14		3
		""";

	@Test
	public void should_export_tabular_report_with_temporal_numerical_and_categorical_columns() throws IOException {
		try (SubjectStore store = new SubjectStore(File.createTempFile("patient", ".oss"))) {
			feed(store);
			Format format = new Format(from, to, Duration.ofDays(7))
				.add(new Column.Temporal(DayOfMonth))
				.add(new Column.Temporal(MonthOfYear))
				.add(new Column.Numerical("temperature", Average))
				.add(new Column.Numerical("temperature", Sum))
				.add(new Column.Numerical("temperature", Min))
				.add(new Column.Numerical("temperature", Max))
				.add(new Column.Numerical("temperature", First))
				.add(new Column.Numerical("temperature", Last))
				.add(new Column.Categorical("cloudy", Mode))
				.add(new Column.Temporal(DayOfWeek));
			SubjectTable table = new SubjectTable(store, format);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			table.export(os);
			assertThat(os.toString()).isEqualTo(expected.trim());
		}
	}

	private void feed(SubjectStore store) {
		store.feed(from.plus(10, DAYS), "test")
				.add("temperature", 19)
				.execute();
		store.feed(from.plus(12, DAYS), "test")
				.add("temperature", 25)
				.add("sky", "cloudy")
				.execute();
		store.feed(from.plus(28, DAYS), "test")
				.add("temperature", 14)
				.add("sky", "rain")
				.execute();
	}
}
