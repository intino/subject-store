package tests.history;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.view.history.ColumnDefinition;
import tests.Storages;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectHistoryView;
import systems.intino.datamarts.subjectstore.calculator.model.filters.MinMaxNormalizationFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.RollingAverageFilter;
import systems.intino.datamarts.subjectstore.view.history.Format;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectHistoryView_ {
	private final static Instant from = Instant.parse("2025-01-01T00:00:00Z");
	private final static Instant to = Instant.parse("2025-02-01T00:00:00Z");
	private final static String expected = """
		2025.0	1.0	1.3817732906760363	0.0	0.0	0.0				0.0	0.0
		2025.0	1.0	1.5296605524915217	48.0	24.0	1.0		28.0	cloudy	1.0	100.0
		2025.0	1.0	1.1905901460252566	0.0	0.0	0.0	8.0			0.0	0.0
		2025.0	1.0	0.5314509965777359	0.0	0.0	0.0	8.0			0.0	0.0
		2025.0	1.0	-0.12333157834482777	18.0	18.0	0.375	6.0	18.0	rain	1.0	37.5
		""";

	@Test
	public void should_export_to_tabular_report_with_format_as_object() throws IOException {
		File file = File.createTempFile("xyz", ":patient.oss");
		SubjectHistory history = new SubjectHistory("map", Storages.in(file));
		feed(history);
		Format format = new Format(from, to, Duration.ofDays(7))
			.add("Year","ts.year")
			.add("Month","ts.month-of-year")
			.add("Day","sin(ts.day-of-month)+cos(ts.month-of-year)")
			.add("TotalTemp","temperature.sum")
			.add("AvgTemp","temperature.average")
			.add("NormTemp","TotalTemp", new MinMaxNormalizationFilter())
			.add("Trend","AvgTemp", new RollingAverageFilter(3))
			.add("LastTemp","temperature.last")
			.add("SkyMode","sky.mode")
			.add("SkyCount","sky.count")
			.add("NewTemp","NormTemp * 100");
		SubjectHistoryView view = new SubjectHistoryView(history, format);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		view.exportTo(os);
		assertThat(os.toString()).isEqualTo(expected);
	}

	@Test
	public void should_export_to_tabular_report_with_format_as_string() throws IOException {
		File file = File.createTempFile("xyz", ":patient.oss");
		SubjectHistory history = new SubjectHistory("map", Storages.in(file));
			feed(history);
			String format = """
			rows:
			  from: 2025-01
			  to: 2025-02
			  period: P7D
			
			columns:
			  - name: "Year"
			    calc: "ts.year"
			
			  - name: "Month"
			    calc: "ts.month-of-year"
			
			  - name: "Day"
			    calc: "sin(ts.day-of-month)+cos(ts.month-of-year)"
			
			  - name: "TotalTemp"
			    calc: "temperature.sum"
			
			  - name: "AvgTemp"
			    calc: "temperature.average"
			
			  - name: "NormTemp"
			    calc: "TotalTemp"
			    filters: ["MinMaxNormalization"]
			
			  - name: "Trend"
			    calc: "AvgTemp"
			    filters: ["RollingAverage:3"]
			
			  - name: "LastTemp"
			    calc: "temperature.last"
			
			  - name: "SkyMode"
			    calc: "sky.mode"
			
			  - name: "SkyCount"
			    calc: "sky.count"
			
			  - name: "NewTemp"
			    calc: "NormTemp * 100"
			""";
		SubjectHistoryView view = new SubjectHistoryView(history, format);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			view.exportTo(os);
			assertThat(os.toString()).isEqualTo(expected);

	}

	private void feed(SubjectHistory history) {
		history.on(from.plus(10, DAYS), "test")
				.put("temperature", 20)
				.terminate();
		history.on(from.plus(12, DAYS), "test")
				.put("temperature", 28)
				.put("sky", "cloudy")
				.terminate();
		history.on(from.plus(28, DAYS), "test")
				.put("temperature", 18)
				.put("sky", "rain")
				.terminate();
	}
}
