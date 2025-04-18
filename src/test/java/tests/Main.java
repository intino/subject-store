package tests;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectStore;
import systems.intino.datamarts.subjectstore.TimeSpan;
import systems.intino.datamarts.subjectstore.model.signals.NumericalSignal;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.File;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.datamarts.subjectstore.TimeReference.today;

public class Main {
	@Test
	public void name() {
		File file = new File("buildings.iss");
		if (file.exists()) file.delete();
		try (SubjectStore store = new SubjectStore(Storages.in(file))) {
			store.has("taj-mahal", "building");
			Subject s = store.create("taj-mahal", "building");
			Subject taj = s.rename("taj mahal");
			taj.index()
					.set("name", "taj mahal")
					.set("year", 1648)
					.put("city", "agra")
					.put("country", "India")
					.put("continent", "Asia")
					.terminate();
			taj.create("domme", "detail").index()
					.set("shape", "onion")
					.set("height", 35)
					.set("material", "white marble")
					.terminate();
			taj.create("minaret", "detail").index()
					.set("count", 4)
					.set("height", 40)
					.terminate();

			try (SubjectHistory history = taj.history()) {
				history.on(today(), "website")
						.put("state", "open")
						.put("visitants", 2405)
						.put("income", 28400)
						.terminate();
				NumericalSignal visitants = history.query()
						.number("visitants")
						.get(TimeSpan.LastYearWindow);
				visitants.summary().mean();

				history.query()
						.text("state")
						.get(Instant.parse("2025-01-01T00:00:00Z"), today(1))
						.summary()
						.mode();
			}




			Subject alhambra = store.create("alhambra", "building");
			alhambra.index()
					.set("name", "alhambra")
					.set("year", 889)
					.put("city", "granada")
					.put("country", "Spain")
					.put("continent", "Europe")
					.terminate();

			alhambra.create("courtyard", "detail").index()
					.set("name", "Court of the Lions")
					.set("style", "Islamic")
					.terminate();

			alhambra.create("tower", "detail").index()
					.set("name", "Torre de la Vela")
					.set("height", 26)
					.set("function", "watchtower")
					.terminate();

			Subject burj = store.create("burj khalifa", "building");
			burj.index()
					.set("name", "burj khalifa")
					.set("year", 2010)
					.put("city", "dubai")
					.put("country", "United Arab Emirates")
					.put("continent", "Asia")
					.terminate();

			burj.create("spire", "detail").index()
					.set("height", 829.8)
					.set("function", "aesthetic and communication")
					.terminate();

			burj.create("observation deck", "detail").index()
					.set("name", "At the Top")
					.set("floor", 148)
					.set("height", 555)
					.terminate();

			assertThat(store.subjects().collect().size()).isEqualTo(9);
			assertThat(store.subjects().roots().size()).isEqualTo(3);
			assertThat(store.subjects("building").collect().size()).isEqualTo(3);
			assertThat(store.subjects("building").with("country", "Spain").collect().size()).isEqualTo(1);
			assertThat(store.subjects("building").with("continent", "Asia").collect().size()).isEqualTo(2);
			assertThat(store.subjects("building").roots().size()).isEqualTo(3);
			assertThat(store.subjects("detail").collect().size()).isEqualTo(6);
			assertThat(store.subjects("detail").roots().size()).isEqualTo(0);
			assertThat(store.subjects("patient").collect().size()).isEqualTo(0);
		}

		try (SubjectStore store = new SubjectStore(Storages.in(file))) {
			assertThat(store.subjects().collect().size()).isEqualTo(9);
			assertThat(store.subjects().roots().size()).isEqualTo(3);
			assertThat(store.subjects("building").collect().size()).isEqualTo(3);
			assertThat(store.subjects("building").with("country", "Spain").collect().size()).isEqualTo(1);
			assertThat(store.subjects("building").with("continent", "Asia").collect().size()).isEqualTo(2);
			assertThat(store.subjects("building").roots().size()).isEqualTo(3);
			assertThat(store.subjects("detail").collect().size()).isEqualTo(6);
			assertThat(store.subjects("detail").roots().size()).isEqualTo(0);
			assertThat(store.subjects("patient").collect().size()).isEqualTo(0);

			store.get("taj mahal", "building").drop();
			assertThat(store.subjects().collect().size()).isEqualTo(6);
			assertThat(store.subjects().roots().size()).isEqualTo(2);
			assertThat(store.subjects("building").collect().size()).isEqualTo(2);
			assertThat(store.subjects("building").with("country", "Spain").collect().size()).isEqualTo(1);
			assertThat(store.subjects("building").with("continent", "Asia").collect().size()).isEqualTo(1);
			assertThat(store.subjects("building").roots().size()).isEqualTo(2);
			assertThat(store.subjects("detail").collect().size()).isEqualTo(4);
			assertThat(store.subjects("detail").roots().size()).isEqualTo(0);
		}
		try (SubjectStore store = new SubjectStore(Storages.in(file))) {
			assertThat(store.subjects().collect().size()).isEqualTo(6);
			assertThat(store.subjects().roots().size()).isEqualTo(2);
			assertThat(store.subjects("building").collect().size()).isEqualTo(2);
			assertThat(store.subjects("building").with("country", "Spain").collect().size()).isEqualTo(1);
			assertThat(store.subjects("building").where("continent").contains("Asia").size()).isEqualTo(1);
			assertThat(store.subjects("building").where("year").matches(v-> toNumber(v) > 1900).size()).isEqualTo(1);
			assertThat(store.subjects("building").roots().size()).isEqualTo(2);
			assertThat(store.subjects("detail").collect().size()).isEqualTo(4);
			assertThat(store.subjects("detail").roots().size()).isEqualTo(0);
		}
	}

	private int toNumber(String value) {
		return Integer.parseInt(value);
	}
}
