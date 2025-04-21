package tests.history;

import org.junit.Test;
import tests.Storages;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.model.Signal;

import java.io.*;
import java.time.Instant;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.datamarts.subjectstore.TimeReferences.Legacy;
import static systems.intino.datamarts.subjectstore.TimeReferences.today;
import static systems.intino.datamarts.subjectstore.TimeSpan.*;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectHistory_ {
	private static final Instant now = Instant.now().truncatedTo(DAYS);
	private static final Instant day = Instant.parse("2025-03-25T00:00:00Z");
	private static final String categories = "DEPOLARISE";

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_handle_empty_history() throws Exception {
		File file = new File("patient.oss");
		try (SubjectHistory history = new SubjectHistory("123.patient", Storages.in(file))) {
			assertThat(history.name()).isEqualTo("123");
			assertThat(history.type()).isEqualTo("patient");
			assertThat(history.typedName()).isEqualTo("123.patient");
			assertThat(history.size()).isEqualTo(0);
			assertThat(history.exists("field")).isFalse();
			assertThat(history.current().number("field")).isNull();
			assertThat(history.current().text("field")).isNull();
			assertThat(history.query().text("field").get()).isNull();
			assertThat(history.legacyExists()).isFalse();
			assertThat(history.bigbangExists()).isFalse();
			assertThat(history.instants()).isEmpty();
		}
		finally {
			file.delete();
		}
	}

	@Test
	public void should_ignore_feed_without_data() throws Exception {
		File file = File.createTempFile("port", ".oss");
		try (SubjectHistory history = new SubjectHistory("00000", Storages.in(file))) {
			history.on(Instant.now(), "Skip").terminate();
			assertThat(history.size()).isEqualTo(0);
		}
	}

	@Test
	public void should_return_most_recent_get_as_current() throws Exception {
		File file = File.createTempFile("patient", ".oss");
		try (SubjectHistory history = new SubjectHistory("12345.patient", Storages.in(file))) {
			feed_batch(history);
			test_batch(history);
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_dump_and_restore_events() throws Exception {
		File file = new File("patient.oss");
		OutputStream os = new ByteArrayOutputStream();
		try (SubjectHistory history = new SubjectHistory("12345.patient", Storages.in(file))) {
			feed_batch(history);
			history.dump(os);
		}
		finally {
			file.delete();
		}
		String dump = os.toString();
		test_dump(dump);
		InputStream is = new ByteArrayInputStream(dump.getBytes());
		 try (SubjectHistory history = new SubjectHistory("12345.patient", Storages.in(file)).restore(is)) {
			history.restore(is);
			test_batch(history);
		}
		finally {
			file.delete();
		}
	}


	@Test
	public void should_store_legacy_values() throws Exception {
		File file = File.createTempFile("port", ".oss");
		try (SubjectHistory history = new SubjectHistory("00000", Storages.in(file))) {
			history.on(Legacy, "UN:all-ports")
					.put("Country", "China")
					.put("Latitude", 31_219832454L)
					.put("Longitude", 121_486998052L)
					.terminate();
			test_stored_legacy_values(history);
		}
		try (SubjectHistory history = new SubjectHistory("00000", Storages.in(file))) {
			test_stored_legacy_values(history);
		}
	}

	private static void test_stored_legacy_values(SubjectHistory history) {
		assertThat(history.size()).isEqualTo(1);
		assertThat(history.first()).isEqualTo(Legacy);
		assertThat(history.last()).isEqualTo(Legacy);
		assertThat(history.tags()).containsExactly("Country", "Latitude", "Longitude");
		assertThat(history.ss(0)).isEqualTo("UN:all-ports");
		assertThat(history.exists("Country")).isTrue();
		assertThat(history.exists("Latitude")).isTrue();
		assertThat(history.exists("Longitude")).isTrue();
		assertThat(history.query().text("Country").get()).isNull();
		assertThat(history.query().text("Latitude").get()).isEqualTo(null);
		assertThat(history.query().text("Longitude").get()).isEqualTo(null);

		assertThat(history.query().text("Country").get(LegacyPhase).values()).containsExactly("China");
		assertThat(history.query().text("Country").get(BigBangPhase).values()).containsExactly();
		assertThat(history.query().text("Country").get(LastYearWindow).values()).containsExactly();
		assertThat(history.query().text("Country").get(LastMonthWindow).values()).containsExactly();
		assertThat(history.query().text("Country").get(LastDayWindow).values()).containsExactly();
		assertThat(history.query().text("Country").get(LastHourWindow).values()).containsExactly();
		assertThat(history.query().number("Latitude").get(history.first(), history.last()).values()).containsExactly(31_219832454L);
		assertThat(history.query().number("Latitude").get(LegacyPhase).values()).containsExactly(31_219832454L);
		assertThat(history.query().number("Latitude").get(BigBangPhase).values()).containsExactly();
		assertThat(history.query().number("Latitude").get(ThisYear).values()).containsExactly();
		assertThat(history.query().number("Latitude").get(ThisMonth).values()).containsExactly();
		assertThat(history.query().number("Latitude").get(Today).values()).containsExactly();
		assertThat(history.query().number("Latitude").get(ThisHour).values()).containsExactly();
		assertThat(history.query().number("Longitude").get(LegacyPhase).values()).containsExactly(121_486998052L);
		assertThat(history.query().number("Longitude").get(BigBangPhase).values()).containsExactly();

		assertThat(history.legacyExists()).isTrue();
		assertThat(history.bigbangExists()).isFalse();
		assertThat(history.legacyPending()).isTrue();
	}

	@Test
	public void should_store_features() throws Exception {
		File file = File.createTempFile("port", ".oss");
		try (SubjectHistory history = new SubjectHistory("00000", Storages.in(file))) {
			history.on(now, "UN:all-ports")
					.put("Country", "China")
					.put("Latitude", 31.219832454)
					.put("Longitude", 121.486998052)
					.terminate();
			test_stored_features(history);
		}
		try (SubjectHistory history = new SubjectHistory("00000", Storages.in(file))) {
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
		assertThat(history.query().number("Latitude").get()).isEqualTo(get(0, now, 31.219832454));
		assertThat(history.query().number("Longitude").get()).isEqualTo(get(0, now, 121.486998052));
		assertThat(history.query().number("Longitude").get(today(), today(1)).values()).containsExactly(121.486998052);
		assertThat(history.query().text("Country").get()).isEqualTo(get(0, now, "China"));
		assertThat(history.query().text("Country").get()).isEqualTo(get(0, now, "China"));
		assertThat(history.query().text("Country").get(today(), today(1)).count()).isEqualTo(1);
		assertThat(history.query().text("Country").get(today(), today(1)).values()).containsExactly("China");
		assertThat(history.instants()).containsExactly(now);
	}

	@Test
	public void should_store_time_series() throws Exception {
		File file = File.createTempFile("port", ".oss");
		try (SubjectHistory history = new SubjectHistory("00000", Storages.in(file))) {
			feed_time_series(history);
			test_stored_time_series(history);
			System.gc();
		}
		try (SubjectHistory history = new SubjectHistory("00000", Storages.in(file))) {
			test_stored_time_series(history);
		}
	}

	@Test
	public void should_create_memory_databases() throws Exception {
		try (SubjectHistory history = new SubjectHistory("00000", Storages.inMemory())) {
			feed_time_series(history);
			test_stored_time_series(history);
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_include_several_subjects() throws Exception {
		File file = new File("subjects.oss");
		String connection = Storages.in(file);
		if (file.exists()) file.delete();
		try {
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
		} finally {
			file.delete();
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
		assertThat(history.query().number("Vessels").get()).isEqualTo(get(9, today(9), 1990.0));
		assertThat(history.query().number("Vessels").get(today(200), today(300)).isEmpty()).isTrue();
		assertThat(history.query().number("Vessels").get(today(-200), today(-100)).isEmpty()).isTrue();
		assertThat(history.query().number("Vessels").all().values()).containsExactly(1900L, 1910L, 1920L, 1930L, 1940L, 1950L, 1960L, 1970L, 1980L, 1990L);
		assertThat(history.query().text("State").all().values()).containsExactly("D", "E", "P", "O", "L", "A", "R", "I", "S", "E");
		assertThat(history.query().text("State").all().distinct()).containsExactly("D", "E", "P", "O", "L", "A", "R", "I", "S");
		assertThat(history.query().text("State").get()).isEqualTo(get(9, today(9), "E"));
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
	private static <T> Signal.Point<T> get(int feed, Instant instant, T value) {
		return new Signal.Point<>(feed, instant, value);
	}

	private static void test_batch(SubjectHistory history) {
		assertThat(history.type()).startsWith("patient");
		assertThat(history.name()).isEqualTo("12345");
		assertThat(history.current().number("hemoglobin")).isEqualTo(145.0);
		Signal.Point<Number> actual = history.query().number("hemoglobin").get();
		assertThat(actual.value()).isEqualTo(145.0);
		assertThat(history.instants()).containsExactly(day.plus(-20, DAYS), day.plus(-5, DAYS), day.plus(-3, DAYS), day);
	}

	//TODO test de DROP

}
