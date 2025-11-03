package tests.misc;

import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectStore;
import systems.intino.datamarts.subjectstore.TimeSpan;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.model.signals.CategoricalSignal;
import systems.intino.datamarts.subjectstore.model.signals.NumericalSignal;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.util.Files.delete;

public class SubjectHistoryMonitor2_ {

	private static final File TMP = new File("temp");

	public static void main(String[] args) throws IOException, SQLException {
		delete(TMP);
		TMP.mkdirs();

		Connection connection = DriverManager.getConnection("jdbc:sqlite:" + new File(TMP, "buildings2.iss"));
		connection.setAutoCommit(false);

		SubjectStore store = new SubjectStore(new File(TMP, "index2.triples"))
				.connection(connection);

		for(int i = 0;i < 100;i++) {
			store.create("b" + i, "building");
		}
		store.seal();

		while(true) {
			for(int i = 0;i < 100;i++) {
				Subject subject = store.open("b" + i, "building");
				write(store, subject);
				read(store, subject);
			}
			sleep(10);
		}
	}

	private static void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static void read(SubjectStore store, Subject subject) {
		try(SubjectHistory history = store.historyOf(subject)) {
			NumericalSignal visitants = history.query()
					.number("visitants")
					.get(TimeSpan.LastMonthWindow);

			double average = visitants.summary().mean();

			CategoricalSignal states = history.query()
					.text("state")
					.get(LocalDateTime.now().minusYears(4).toInstant(ZoneOffset.UTC), Instant.now());

			String state = states.summary().mode();

			int timesClosed = states.summary().frequency("closed");

			System.out.println(average + state + timesClosed);
		}
	}

	private static void write(SubjectStore store, Subject subject) {
		try(SubjectHistory history = store.historyOf(subject)) {
			history.on(Instant.now(), "website")
					.put("state", ThreadLocalRandom.current().nextBoolean() ? "open" : "closed")
					.put("visitants", ThreadLocalRandom.current().nextInt(0, 1000))
					.put("income", ThreadLocalRandom.current().nextInt(0, 100000000))
					.terminate();
		}
	}
}
