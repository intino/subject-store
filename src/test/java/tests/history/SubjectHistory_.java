package tests.history;

import org.junit.Test;
import tests.Jdbc;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.model.Signal;

import java.io.*;
import java.time.Instant;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.datamarts.subjectstore.TimeReferences.today;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectHistory_ {
	private static final Instant now = Instant.now().truncatedTo(DAYS);
	private static final Instant day = Instant.parse("2025-03-25T00:00:00Z");
	private static final String categories = "DEPOLARISE";

	@Test
	public void should_handle_empty_history() throws Exception {
		try (SubjectHistory history = new SubjectHistory("123.patient", Jdbc.sqlite())) {
			assertThat(history.name()).isEqualTo("123");
			assertThat(history.type()).isEqualTo("patient");
			assertThat(history.typedName()).isEqualTo("123.patient");
			assertThat(history.size()).isEqualTo(0);
			assertThat(history.exists("field")).isFalse();
			assertThat(history.current().point("field")).isNull();
			assertThat(history.instants()).isEmpty();
		}
	}

	@Test
	public void should_ignore_feed_without_data() throws Exception {
		try (SubjectHistory history = new SubjectHistory("00000", Jdbc.sqlite())) {
			history.on(Instant.now(), "Skip").terminate();
			assertThat(history.size()).isEqualTo(0);
		}
	}

	@Test
	public void should_return_most_recent_get_as_current() throws Exception {
		try (SubjectHistory history = new SubjectHistory("12345.patient", Jdbc.sqlite())) {
			feed_batch(history);
			test_batch(history);
		}
	}

	@Test
	public void should_dump_and_restore_events() throws Exception {
		OutputStream os = new ByteArrayOutputStream();
		try (SubjectHistory history = new SubjectHistory("12345.patient", Jdbc.sqlite())) {
			feed_batch(history);
			history.dump(os);
		}
		String dump = os.toString();
		test_dump(dump);
		InputStream is = new ByteArrayInputStream(dump.getBytes());
		try (SubjectHistory history = new SubjectHistory("12345.patient", Jdbc.sqlite()).restore(is)) {
			history.restore(is);
			test_batch(history);
		}
	}


	@Test
	public void should_store_features() throws Exception {
		String store = Jdbc.sqlite();
		try (SubjectHistory history = new SubjectHistory("00000", store)) {
			history.on(now, "UN:all-ports")
					.put("Country", "China")
					.put("Latitude", 31.219832454)
					.put("Longitude", 121.486998052)
					.terminate();
			test_stored_features(history);
		}
		try (SubjectHistory history = new SubjectHistory("00000", store)) {
			test_stored_features(history);
		}
	}

	private static void test_stored_features(SubjectHistory history) {
		assertThat(history.name()).isEqualTo("00000");
		assertThat(history.size()).isEqualTo(1);
		assertThat(history.first()).isEqualTo(now);
		assertThat(history.last()).isEqualTo(now);
		assertThat(history.tags()).containsExactly("Country", "Latitude", "Longitude");
		assertThat(history.ss(0)).isEqualTo("UN:all-ports");
		assertThat(history.current().number("Latitude")).isEqualTo(31.219832454);

		assertThat(history.current().point("Latitude")).isEqualTo(get(0, now, 31.219832454));
		assertThat(history.current().point("Longitude")).isEqualTo(get(0, now, 121.486998052));
		assertThat(history.query().number("Longitude").get(today(), today(1)).values()).containsExactly(121.486998052);
		assertThat(history.current().point("Country")).isEqualTo(get(0, now, "China"));
		assertThat(history.query().text("Country").get(today(), today(1)).count()).isEqualTo(1);
		assertThat(history.query().text("Country").get(today(), today(1)).values()).containsExactly("China");
		assertThat(history.query().text("Country").get(today(), today(1)).instants().length).isEqualTo(1);
		assertThat(history.instants()).containsExactly(now);
	}

	@Test
	public void should_store_time_series() throws Exception {
		String module = Jdbc.sqlite();
		try (SubjectHistory history = new SubjectHistory("00000", module)) {
			feed_time_series(history);
			test_stored_time_series(history);
		}
		try (SubjectHistory history = new SubjectHistory("00000", module)) {
			test_stored_time_series(history);
		}
	}

	@Test
	public void should_create_memory_databases() {
		try (SubjectHistory history = new SubjectHistory("00000", Jdbc.memory())) {
			feed_time_series(history);
			test_stored_time_series(history);
		}
	}

	@Test
	public void should_include_several_subjects() throws Exception {
		String connection = Jdbc.sqlite();
		SubjectHistory[] histories = new SubjectHistory[]{
				new SubjectHistory("00001", connection),
				new SubjectHistory("00002", connection),
				new SubjectHistory("00003", connection),
				new SubjectHistory("00004", connection)
		};
		for (SubjectHistory history : histories) {
			feed_time_series(history);
			test_stored_time_series(history);
		}
	}

	private static void feed_time_series(SubjectHistory history) {
		for (int i = 0; i < 10; i++) {
			history.on(today(i), "AIS:movements-" + i)
					.put("Vessels", 1900 + i * 10)
					.put("State", categories.substring(i, i + 1))
					.terminate();
		}
	}

	private void test_stored_time_series(SubjectHistory history) {
		assertThat(history.size()).isEqualTo(10);
		assertThat(history.first()).isEqualTo(today());
		assertThat(history.last()).isEqualTo(today(9));
		assertThat(history.tags()).containsExactly("Vessels", "State");
		assertThat(history.ss(0)).isEqualTo("AIS:movements-0");
		assertThat(history.ss(9)).isEqualTo("AIS:movements-9");
		assertThat(history.current().number("Vessels")).isEqualTo(get(9, today(9), 1990.0).value());
		assertThat(history.query().number("Vessels").get(today(200), today(300)).isEmpty()).isTrue();
		assertThat(history.query().number("Vessels").get(today(-200), today(-100)).isEmpty()).isTrue();
		assertThat(history.query().number("Vessels").all().values()).containsExactly(1900L, 1910L, 1920L, 1930L, 1940L, 1950L, 1960L, 1970L, 1980L, 1990L);
		assertThat(history.query().number("Vessels").all().instants().length).isEqualTo(10);
		assertThat(history.query().text("State").all().values()).containsExactly("D", "E", "P", "O", "L", "A", "R", "I", "S", "E");
		assertThat(history.query().text("State").all().distinct()).containsExactly("D", "E", "P", "O", "L", "A", "R", "I", "S");
		assertThat(history.current().point("State")).isEqualTo(get(9, today(9), "E"));
		assertThat(history.query().text("State").get(today(0), today(10)).summary().mode()).isEqualTo("E");
	}

	private static void feed_batch(SubjectHistory history) {
		SubjectHistory.Batch batch = history.batch();
		batch.on(day, "HMG-2")
				.put("hemoglobin", 145)
				.terminate();

		batch.on(day.plus(-5, DAYS), "HMG-1")
				.put("hemoglobin", 130)
				.terminate();

		batch.on(day.plus(-3, DAYS), "HMG-B")
				.put("hemoglobin", 115)
				.terminate();

		batch.on(day.plus(-20, DAYS), "HMG-L")
				.put("hemoglobin", 110)
				.terminate();

		batch.terminate();
	}

	private static void test_dump(String dump) {
		assertThat(dump).isEqualTo("""
				[patient]
				ts=2025-03-25T00:00:00Z
				ss=HMG-2
				id=12345.patient
				hemoglobin=145.0
				[patient]
				ts=2025-03-20T00:00:00Z
				ss=HMG-1
				id=12345.patient
				hemoglobin=130.0
				[patient]
				ts=2025-03-22T00:00:00Z
				ss=HMG-B
				id=12345.patient
				hemoglobin=115.0
				[patient]
				ts=2025-03-05T00:00:00Z
				ss=HMG-L
				id=12345.patient
				hemoglobin=110.0
				"""
		);
	}

	@SuppressWarnings("SameParameterValue")
	private static <T> Signal.Point<T> get(int feed, Instant instant, T value) {
		return new Signal.Point<>(feed, instant, value);
	}

	private static void test_batch(SubjectHistory history) {
		assertThat(history.type()).startsWith("patient");
		assertThat(history.name()).isEqualTo("12345");
		assertThat(history.current().number("hemoglobin")).isEqualTo(145.0);
		Signal.Point<?> actual = history.current().point("hemoglobin");
		assertThat(actual.value()).isEqualTo(145.0);
		assertThat(history.instants()).containsExactly(day.plus(-20, DAYS), day.plus(-5, DAYS), day.plus(-3, DAYS), day);
	}

}
