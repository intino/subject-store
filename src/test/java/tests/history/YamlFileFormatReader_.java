package tests.history;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.calculator.model.filters.MinMaxNormalizationFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.RollingAverageFilter;
import systems.intino.datamarts.subjectstore.history.view.Format;
import systems.intino.datamarts.subjectstore.history.view.format.YamlFormatReader;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;


@SuppressWarnings("NewClassNamingConvention")
public class YamlFileFormatReader_ {
	@Test
	public void should_parse_format_periods_of_days() throws Exception {
		String content = """
		rows:
		    from: "2025-01-01T00:00:00Z"
		    to: "2025-03-01T00:00:00Z"
		    period: "P7D"
		""";

		Format format = new YamlFormatReader(content).read();
		assertThat(format).isNotNull();
		assertThat(format.from()).isEqualTo(LocalDate.of(2025, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.to()).isEqualTo(LocalDate.of(2025, 3, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.duration()).isEqualTo(Period.ofDays(7));
		assertThat(format.columns()).hasSize(0);
	}

	@Test
	public void should_parse_format_periods_of_hours() throws Exception {
		String content = """
		rows:
		  from: 2025-01-01
		  to: 2025-03-01
		  period: PT2H
		""";

		Format format = new YamlFormatReader(content).read();
		assertThat(format).isNotNull();
		assertThat(format.from()).isEqualTo(LocalDate.of(2025, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.to()).isEqualTo(LocalDate.of(2025, 3, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.duration()).isEqualTo(Duration.ofHours(2));
		assertThat(format.columns()).hasSize(0);
	}

	@Test
	public void should_parse_format_periods_of_months_with_monthly_date_resolution() throws Exception {
		String content = """
		rows:
		  from: 2025-01
		  to: 2025-03
		  period: P4M
		""";

		Format format = new YamlFormatReader(content).read();
		assertThat(format).isNotNull();
		assertThat(format.from()).isEqualTo(LocalDate.of(2025, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.to()).isEqualTo(LocalDate.of(2025, 3, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.duration()).isEqualTo(Period.ofMonths(4));
		assertThat(format.columns()).hasSize(0);
	}

	@Test
	public void should_parse_format_periods_of_years_with_yearly_date_resolution() throws Exception {
		String content = """
		rows:
		  from: 2025
		  to: 2028
		  period: P1Y
		""";

		Format format = new YamlFormatReader(content).read();
		assertThat(format).isNotNull();
		assertThat(format.from()).isEqualTo(LocalDate.of(2025, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.to()).isEqualTo(LocalDate.of(2028, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.duration()).isEqualTo(Period.ofYears(1));
		assertThat(format.columns()).hasSize(0);
	}

	@Test
	public void should_parse_columns() {
		String content = """
		rows:
		  from: 2025
		  to: 2028
		  period: P1Y
		
		columns:
		   - name: weight
		     calc: weight / 100
		     filters: [RollingAverage:3, MinMaxNormalization]
		""";

		Format format = new YamlFormatReader(content).read();
		assertThat(format).isNotNull();
		assertThat(format.from()).isEqualTo(LocalDate.of(2025, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.to()).isEqualTo(LocalDate.of(2028, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
		assertThat(format.duration()).isEqualTo(Period.ofYears(1));
		assertThat(format.columns()).hasSize(1);
		assertThat(format.columns().getFirst().name).isEqualTo("weight");
		assertThat(format.columns().getFirst().definition).isEqualTo("weight / 100");
		assertThat(format.columns().getFirst().filters.size()).isEqualTo(2);
		assertThat(format.columns().getFirst().filters.getFirst()).isEqualTo(new RollingAverageFilter(3));
		assertThat(format.columns().getFirst().filters.get(1)).isEqualTo(new MinMaxNormalizationFilter());
	}

}
