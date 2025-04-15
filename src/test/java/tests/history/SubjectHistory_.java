package tests.history;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.SqliteConnection;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.history.model.Point;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.datamarts.subjectstore.TimeReference.Legacy;
import static systems.intino.datamarts.subjectstore.TimeSpan.*;
import static systems.intino.datamarts.subjectstore.TimeReference.today;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectHistory_ {
	private static final Instant now = Instant.now().truncatedTo(DAYS);
	private static final Instant day = Instant.parse("2025-03-25T00:00:00Z");
	private static final String categories = "DEPOLARISE";

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_handle_empty_store() {
		File file = new File("patient.oss");
		try (SubjectHistory store = new SubjectHistory("123.patient", file)) {
			assertThat(store.name()).isEqualTo("123");
			assertThat(store.type()).isEqualTo("patient");
			assertThat(store.typedName()).isEqualTo("123.patient");
			assertThat(store.size()).isEqualTo(0);
			assertThat(store.exists("field")).isFalse();
			assertThat(store.currentNumber("field")).isNull();
			assertThat(store.currentText("field")).isNull();
			assertThat(store.categoricalQuery("field").get()).isNull();
			assertThat(store.legacyExists()).isFalse();
			assertThat(store.bigbangExists()).isFalse();
			assertThat(store.instants()).isEmpty();
		}
		finally {
			file.delete();
		}
	}

	@Test
	public void should_ignore_feed_without_data() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectHistory store = new SubjectHistory("00000", file)) {
			store.on(Instant.now(), "Skip").commit();
			assertThat(store.size()).isEqualTo(0);
		}
	}

	@Test
	public void should_return_most_recent_value_as_current() throws IOException {
		File file = File.createTempFile("patient", ".oss");
		try (SubjectHistory store = new SubjectHistory("12345.patient", file)) {
			feed_batch(store);
			test_batch(store);
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_dump_and_restore_events() throws IOException {
		File file = new File("patient.oss");
		OutputStream os = new ByteArrayOutputStream();
		try (SubjectHistory store = new SubjectHistory("12345.patient", file)) {
			feed_batch(store);
			store.dump(os);
		}
		finally {
			file.delete();
		}
		String dump = os.toString();
		test_dump(dump);
		InputStream is = new ByteArrayInputStream(dump.getBytes());
		try (SubjectHistory store = new SubjectHistory("12345.patient", file)) {
			store.restore(is);
			test_batch(store);
		}
		finally {
			file.delete();
		}
	}


	@Test
	public void should_store_legacy_values() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectHistory store = new SubjectHistory("00000", file)) {
			store.on(Legacy, "UN:all-ports")
					.put("Country", "China")
					.put("Latitude", 31_219832454L)
					.put("Longitude", 121_486998052L)
					.commit();
			test_stored_legacy_values(store);
		}
		try (SubjectHistory store = new SubjectHistory("00000", file)) {
			test_stored_legacy_values(store);
		}
	}

	private static void test_stored_legacy_values(SubjectHistory store) {
		assertThat(store.size()).isEqualTo(1);
		assertThat(store.first()).isEqualTo(Legacy);
		assertThat(store.last()).isEqualTo(Legacy);
		assertThat(store.tags()).containsExactly("Country", "Latitude", "Longitude");
		assertThat(store.ss(0)).isEqualTo("UN:all-ports");
		assertThat(store.exists("Country")).isTrue();
		assertThat(store.exists("Latitude")).isTrue();
		assertThat(store.exists("Longitude")).isTrue();
		assertThat(store.categoricalQuery("Country").get()).isNull();
		assertThat(store.categoricalQuery("Latitude").get()).isEqualTo(null);
		assertThat(store.categoricalQuery("Longitude").get()).isEqualTo(null);

		assertThat(store.categoricalQuery("Country").get(LegacyPhase).values()).containsExactly("China");
		assertThat(store.categoricalQuery("Country").get(BigBangPhase).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").get(LastYearWindow).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").get(LastMonthWindow).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").get(LastDayWindow).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").get(LastHourWindow).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(store.first(), store.last()).values()).containsExactly(31_219832454L);
		assertThat(store.numericalQuery("Latitude").get(LegacyPhase).values()).containsExactly(31_219832454L);
		assertThat(store.numericalQuery("Latitude").get(BigBangPhase).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(ThisYear).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(ThisMonth).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(Today).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(ThisHour).values()).containsExactly();
		assertThat(store.numericalQuery("Longitude").get(LegacyPhase).values()).containsExactly(121_486998052L);
		assertThat(store.numericalQuery("Longitude").get(BigBangPhase).values()).containsExactly();

		assertThat(store.legacyExists()).isTrue();
		assertThat(store.bigbangExists()).isFalse();
		assertThat(store.legacyPending()).isTrue();
	}

	@Test
	public void should_store_features() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectHistory store = new SubjectHistory("00000", file)) {
			store.on(now, "UN:all-ports")
					.put("Country", "China")
					.put("Latitude", 31.219832454)
					.put("Longitude", 121.486998052)
					.commit();
			test_stored_features(store);
		}
		try (SubjectHistory store = new SubjectHistory("00000", file)) {
			test_stored_features(store);
		}
	}

	private static void test_stored_features(SubjectHistory store) {
		assertThat(store.name()).isEqualTo("00000");
		assertThat(store.size()).isEqualTo(1);
		assertThat(store.first()).isEqualTo(now);
		assertThat(store.last()).isEqualTo(now);
		assertThat(store.tags()).containsExactly("Country", "Latitude", "Longitude");
		assertThat(store.ss(0)).isEqualTo("UN:all-ports");
		assertThat(store.currentNumber("Latitude")).isEqualTo(31.219832454);
		assertThat(store.numericalQuery("Latitude").get()).isEqualTo(value(0, now, 31.219832454));
		assertThat(store.numericalQuery("Longitude").get()).isEqualTo(value(0, now, 121.486998052));
		assertThat(store.numericalQuery("Longitude").get(today(), today(1)).values()).containsExactly(121.486998052);
		assertThat(store.categoricalQuery("Country").get()).isEqualTo(value(0, now, "China"));
		assertThat(store.categoricalQuery("Country").get()).isEqualTo(value(0, now, "China"));
		assertThat(store.categoricalQuery("Country").get(today(), today(1)).count()).isEqualTo(1);
		assertThat(store.categoricalQuery("Country").get(today(), today(1)).values()).containsExactly("China");
		assertThat(store.instants()).containsExactly(now);
	}

	@Test
	public void should_store_time_series() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectHistory store = new SubjectHistory("00000", file)) {
			feed_time_series(store);
			test_stored_time_series(store);
		}
		try (SubjectHistory store = new SubjectHistory("00000", file)) {
			test_stored_time_series(store);
		}
	}

	@Test
	public void should_create_memory_databases() {
		try (SubjectHistory store = new SubjectHistory("00000")) {
			feed_time_series(store);
			test_stored_time_series(store);
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_include_several_subjects() throws SQLException {
		File file = new File("subjects.oss");
		if (file.exists()) file.delete();
		try (Connection connection = SqliteConnection.from(file)) {
			SubjectHistory[] stores = new SubjectHistory[]{
					new SubjectHistory("00001", connection),
					new SubjectHistory("00002", connection),
					new SubjectHistory("00003", connection),
					new SubjectHistory("00004", connection)
			};
			for (SubjectHistory store : stores) {
				feed_time_series(store);
				test_stored_time_series(store);
			}
		} finally {
			file.delete();
		}
	}

	private static void feed_time_series(SubjectHistory store) {
		for (int i = 0; i < 10; i++) {
			store.on(today(i), "AIS:movements-" + i)
					.put("Vessels", 1900 + i * 10)
					.put("State", categories.substring(i, i + 1))
					.commit();
		}
	}

	private void test_stored_time_series(SubjectHistory store) {
		assertThat(store.size()).isEqualTo(10);
		assertThat(store.first()).isEqualTo(today());
		assertThat(store.last()).isEqualTo(today(9));
		assertThat(store.tags()).containsExactly("Vessels", "State");
		assertThat(store.ss(0)).isEqualTo("AIS:movements-0");
		assertThat(store.ss(9)).isEqualTo("AIS:movements-9");
		assertThat(store.numericalQuery("Vessels").get()).isEqualTo(value(9, today(9), 1990.0));
		assertThat(store.numericalQuery("Vessels").get(today(200), today(300)).isEmpty()).isTrue();
		assertThat(store.numericalQuery("Vessels").get(today(-200), today(-100)).isEmpty()).isTrue();
		assertThat(store.numericalQuery("Vessels").all().values()).containsExactly(1900L, 1910L, 1920L, 1930L, 1940L, 1950L, 1960L, 1970L, 1980L, 1990L);
		assertThat(store.categoricalQuery("State").all().values()).containsExactly("D", "E", "P", "O", "L", "A", "R", "I", "S", "E");
		assertThat(store.categoricalQuery("State").all().distinct()).containsExactly("D", "E", "P", "O", "L", "A", "R", "I", "S");
		assertThat(store.categoricalQuery("State").get()).isEqualTo(value(9, today(9), "E"));
		assertThat(store.categoricalQuery("State").get(today(0), today(10)).summary().mode()).isEqualTo("E");
	}

	private static void feed_batch(SubjectHistory store) {
		SubjectHistory.Batch batch = store.batch();
		batch.on(day, "HMG-2")
				.put("hemoglobin", 145)
				.commit();

		batch.on(day.plus(-5, DAYS), "HMG-1")
				.put("hemoglobin", 130)
				.commit();

		batch.on(day.plus(-3, DAYS), "HMG-B")
				.put("hemoglobin", 115)
				.commit();

		batch.on(day.plus(-20, DAYS), "HMG-L")
				.put("hemoglobin", 110)
				.commit();

		batch.commit();
	}

	private static void test_dump(String dump) {
		assertThat(dump).isEqualTo("""
				[subject]
				ts=2025-03-25T00:00:00Z
				ss=HMG-2
				id=12345.patient
				hemoglobin=145.0
				[subject]
				ts=2025-03-20T00:00:00Z
				ss=HMG-1
				id=12345.patient
				hemoglobin=130.0
				[subject]
				ts=2025-03-22T00:00:00Z
				ss=HMG-B
				id=12345.patient
				hemoglobin=115.0
				[subject]
				ts=2025-03-05T00:00:00Z
				ss=HMG-L
				id=12345.patient
				hemoglobin=110.0
				"""
		);
	}

	@SuppressWarnings("SameParameterValue")
	private static <T> Point<T> value(int feed, Instant instant, T value) {
		return new Point<>(feed, instant, value);
	}

	private static void test_batch(SubjectHistory store) {
		assertThat(store.type()).startsWith("patient");
		assertThat(store.name()).isEqualTo("12345");
		assertThat(store.currentNumber("hemoglobin")).isEqualTo(145.0);
		Point<Double> actual = store.numericalQuery("hemoglobin").get();
		assertThat(actual.value()).isEqualTo(145);
		assertThat(store.instants()).containsExactly(day.plus(-20, DAYS), day.plus(-5, DAYS), day.plus(-3, DAYS), day);
	}
}
