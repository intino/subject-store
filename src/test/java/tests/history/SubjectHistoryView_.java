package tests.history;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import systems.intino.datamarts.subjectstore.calculator.model.Filter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.LagFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.LeadFilter;
import systems.intino.datamarts.subjectstore.view.history.format.HistoryFormat;
import tests.Jdbc;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectHistoryView;
import systems.intino.datamarts.subjectstore.calculator.model.filters.MinMaxNormalizationFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.RollingAverageFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;

import static java.lang.Math.sin;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.PERIOD;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectHistoryView_ {
	private final static Instant from = Instant.parse("2025-01-01T00:00:00Z");
	private final static Instant to = Instant.parse("2025-02-01T00:00:00Z");
	private final static String expected = """
		date	month	day-formula	temp-total	temp-average	temp-norm	temp-trend	temp-last	sky-mode	sky-count	temp-norm%
		20250101	1.0	1.3817732906760363	0.0	0.0	0.0				0.0	0.0
		20250108	1.0	1.5296605524915217	48.0	24.0	1.0		28.0	cloudy	1.0	100.0
		20250115	1.0	1.1905901460252566	0.0	0.0	0.0	8.0			0.0	0.0
		20250122	1.0	0.5314509965777359	0.0	0.0	0.0	8.0			0.0	0.0
		20250129	1.0	-0.12333157834482777	18.0	18.0	0.375	6.0	18.0	rain	1.0	37.5
		""";

	private Connection connection;

	@Before
	public void setUp() throws Exception {
		String url = Jdbc.sqlite();
		this.connection = DriverManager.getConnection(url);
		this.connection.setAutoCommit(false);
	}

	@After
	public void tearDown() throws Exception {
		this.connection.close();
	}
	@Test
	public void should_export_to_tabular_report_with_format_as_object() throws IOException {
		SubjectHistory history = new SubjectHistory("map", connection);
		feed(history);
		HistoryFormat historyFormat = new HistoryFormat(from, to, Duration.ofDays(7))
			.add("date","ts.year-month-day")
			.add("month","ts.month-of-year")
			.add("day-formula","sin(ts.day-of-month)+cos(ts.month-of-year)")
			.add("temp-total","temperature.sum")
			.add("temp-average","temperature.average")
			.add("temp-norm","temp-total", new MinMaxNormalizationFilter())
			.add("temp-trend","temp-average", new RollingAverageFilter(3))
			.add("temp-last","temperature.last")
			.add("sky-mode","sky.mode")
			.add("sky-count","sky.count")
			.add("temp-norm%","temp-norm * 100");
		SubjectHistoryView view = SubjectHistoryView.of(history).with(historyFormat);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		view.export().to(os);
		assertThat(os.toString()).isEqualTo(expected);
	}

	@Test
	public void name() throws IOException {
		SubjectHistory history = new SubjectHistory("map", connection);
		for (int i = 0; i < 2000; i++) {
			SubjectHistory.Batch batch = history.batch();
			batch.on(from.plus(i, HOURS), "test")
					.put("temperature", 20 + 4 * sin(i))
					.terminate();
			batch.terminate();
		}
		SubjectHistoryView view = SubjectHistoryView.of(history)
				.from(from)
				.to(from.plus(2000, HOURS))
				.period(Duration.ofHours(200))
				.add("temperature", "temperature.mean")
				.add("temperature-1", "temperature", new LagFilter(1))
				.add("temperature-2", "temperature", new LagFilter(2))
				.add("temperature-3", "temperature", new LagFilter(3))
				.add("temperature+1", "temperature", new LeadFilter(1))
				.build();
		OutputStream os = new ByteArrayOutputStream();
		view.export().start(3).stop(1).to(os);
		System.out.println(os);

	}

	@Test
	public void should_export_to_tabular_report_with_format_as_string() throws IOException {
		SubjectHistory history = new SubjectHistory("map", connection);
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
			view.export().to(os);
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
