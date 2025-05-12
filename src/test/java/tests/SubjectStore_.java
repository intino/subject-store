package tests;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectHistoryView;
import systems.intino.datamarts.subjectstore.SubjectIndexView;
import systems.intino.datamarts.subjectstore.SubjectStore;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.view.index.Column.Type;

import java.io.*;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@SuppressWarnings({"ResultOfMethodCallIgnored", "NewClassNamingConvention"})
public class SubjectStore_ {
	@Test
	public void should_create_and_persist_subjects_with_hierarchy_and_history() throws IOException {
		File index = new File("index.triples");
		File journal = new File(index.getAbsolutePath() + ".journal");
		try {
			try (SubjectStore store = new SubjectStore(index)) {
				assertThat(store.has("taj-mahal", "building")).isFalse();
				assertThat(store.has("taj-mahal.building")).isFalse();
				assertThat(index.exists()).isFalse();
				assertThat(journal.exists()).isFalse();
				createSubjects(store);
				assertThat(index.exists()).isFalse();
				assertThat(journal.exists()).isTrue();
				test1(store);
			}
			try (SubjectStore store = new SubjectStore(index)) {
				store.seal();
				assertThat(index.exists()).isTrue();
				assertThat(journal.exists()).isFalse();
				test2(store);
				store.open("taj_mahal", "building").drop();
				assertThat(index.exists()).isTrue();
				assertThat(journal.exists()).isTrue();
				assertThat(store.subjects().collect().size()).isEqualTo(6);
				assertThat(store.subjects().isRoot().size()).isEqualTo(2);
				assertThat(store.subjects().type("building").collect().size()).isEqualTo(2);
				assertThat(store.subjects().type("building").where("country").equals("Spain").collect().size()).isEqualTo(1);
				assertThat(store.subjects().type("building").where("continent").equals("Asia").collect().size()).isEqualTo(1);
				assertThat(store.subjects().type("building").isRoot().size()).isEqualTo(2);
				assertThat(store.subjects().type("detail").collect().size()).isEqualTo(4);
				assertThat(store.subjects().type("detail").isRoot().size()).isEqualTo(0);
			}

			try (SubjectStore store = new SubjectStore(index)) {
				store.seal();
				assertThat(index.exists()).isTrue();
				assertThat(journal.exists()).isFalse();
				test3(store);
			}
		}
		finally {
			index.delete();
			journal.delete();
		}
	}

	@Test
	public void should_create_histories() throws IOException {
		File index = new File("index.triples");
		try (SubjectStore store = new SubjectStore(index, Jdbc.sqlite())) {
			createSubjects(store);
			createHistory(store);
			store.seal();
			test1(store);
			test2(store);
			testHistory(store);
		}
		finally {
			index.delete();
		}
	}

	private static void test1(SubjectStore store) {
		Subject building = store.open("burj khalifa", "building");

		assertThat(building.children().type("detail").where("name").contains("Top").collect()).containsOnly(store.open("burj khalifa.building/observation deck.detail"));
		assertThat(building.children().type("detail").where("name").contains("Top").collect()).containsOnly(store.open("burj khalifa.building/observation deck.detail"));
		assertThat(building.children().type("detail").where("name").equals("At the Top").collect()).containsOnly(store.open("burj khalifa.building/observation deck.detail"));
		assertThat(building.children().type("detail").where("name").equals("At the Top").isRoot().collect()).isEmpty();
		assertThat(building.children().type("departament").collect().size()).isEqualTo(0);

		assertThat(store.subjects().collect().size()).isEqualTo(9);
		assertThat(store.subjects().isRoot().size()).isEqualTo(3);
		assertThat(store.subjects().type("building").collect().size()).isEqualTo(3);
		assertThat(store.subjects().type("building").where("country").equals("Spain").collect().size()).isEqualTo(1);
		assertThat(store.subjects().type("building").where("continent").equals("Asia").collect().size()).isEqualTo(2);
		assertThat(store.subjects().type("building").isRoot().size()).isEqualTo(3);
		assertThat(store.subjects().type("detail").isRoot().size()).isEqualTo(0);
		assertThat(store.subjects().type("detail").collect().size()).isEqualTo(6);
	}

	private static void test2(SubjectStore store) {
		assertThat(store.has("alhambra", "building")).isTrue();
		assertThat(store.has("torre del oro", "building")).isFalse();
		assertThat(store.subjects().collect().size()).isEqualTo(9);
		assertThat(store.subjects().isRoot().size()).isEqualTo(3);
		assertThat(store.subjects().type("building").collect().size()).isEqualTo(3);
		assertThat(store.subjects().type("building").where("country").equals("Spain").collect().size()).isEqualTo(1);
		assertThat(store.subjects().type("building").where("continent").equals("Asia").collect().size()).isEqualTo(2);
		assertThat(store.subjects().type("building").isRoot().size()).isEqualTo(3);
		assertThat(store.subjects().type("detail").collect().size()).isEqualTo(6);
		assertThat(store.subjects().type("detail").isRoot().size()).isEqualTo(0);
		assertThat(store.subjects().type("patient").collect().size()).isEqualTo(0);

		Subject building = store.subjects().type("building").where("city").equals("Dubai").first();
		assertThat(building.children().type("detail").where("name").contains("Top").collect()).containsOnly(store.open("burj khalifa.building/observation deck.detail"));
		assertThat(building.children().type("detail").where("name").contains("Top").collect()).containsOnly(store.open("burj khalifa.building/observation deck.detail"));
		assertThat(building.children().type("detail").where("name").equals("At the Top").collect()).containsOnly(store.open("burj khalifa.building/observation deck.detail"));
		assertThat(building.children().type("detail").where("name").equals("At the Top").isRoot().collect()).isEmpty();
		assertThat(building.children().type("departament").collect().size()).isEqualTo(0);
	}

	private void test3(SubjectStore store) {
		assertThat(store.subjects().collect().size()).isEqualTo(6);
		assertThat(store.subjects().isRoot().size()).isEqualTo(2);
		assertThat(store.subjects().type("building").collect().size()).isEqualTo(2);
		assertThat(store.subjects().type("building").where("country").equals("Spain").collect().size()).isEqualTo(1);
		assertThat(store.subjects().type("building").where("continent").contains("Asia").size()).isEqualTo(1);
		assertThat(store.subjects().type("building").where("year").satisfy(v-> toNumber(v) > 1900).size()).isEqualTo(1);
		assertThat(store.subjects().type("building").isRoot().size()).isEqualTo(2);
		assertThat(store.subjects().type("detail").collect().size()).isEqualTo(4);
		assertThat(store.subjects().type("detail").isRoot().size()).isEqualTo(0);
	}

	private void createHistory(SubjectStore store) {
		for (Subject subject : store.subjects().collect()) {
			try (SubjectHistory history = store.historyOf(subject)) {
				history.on("2025", "test").put("visits", 10).terminate();
			}
		}
	}

	private void testHistory(SubjectStore store) {
		for (Subject subject : store.subjects().collect()) {
			try (SubjectHistory history = store.historyOf(subject)) {
				assertThat(history.query().number("visits").all().summary().count()).isEqualTo(1);
				assertThat(history.query().number("visits").all().summary().sum()).isEqualTo(10.0);
			}
		}
	}

	public void should_create_views() throws IOException {
		File index = new File("index.triples");
		try (SubjectStore store = new SubjectStore(index, Jdbc.sqlite())) {
			createSubjects(store);
			SubjectIndexView view = SubjectIndexView
					.of(store.subjects().type("building").collect())
					.add("year", Type.Number)
					.add("city", Type.Text)
					.add("country", Type.Text)
					.build();
			SubjectHistoryView tajMahalHistory = SubjectHistoryView
					.of(store.historyOf("taj_mahal.building"))
					.with(new File("format.yaml"));
			SubjectHistoryView historyView = SubjectHistoryView
					.of(store.historyOf("taj_mahal.building"))
					.from("2010")
					.to("2025-04")
					.duration("P1Y")
					.add("","")
					.build();
		}
	}

	private static void createSubjects(SubjectStore store) {
		Subject s = store.create("taj-mahal", "building");
		Subject taj = s.rename("taj_mahal");
		taj.update()
				.set("name", "taj_mahal")
				.set("year", 1648)
				.put("city", "Agra")
				.put("country", "India")
				.put("continent", "Asia");
		taj.create("domme", "detail").update()
				.set("shape", "onion")
				.set("height", 35)
				.set("material", "white marble");
		taj.create("minaret", "detail").update()
				.set("count", 4)
				.set("height", 40);


		Subject alhambra = store.create("alhambra", "building");
		alhambra.update()
				.set("name", "Alhambra")
				.set("year", 889)
				.put("city", "Granada")
				.put("country", "Spain")
				.put("continent", "Europe");

		alhambra.create("courtyard", "detail").update()
				.set("name", "Court of the Lions")
				.set("style", "Islamic");

		alhambra.create("tower", "detail").update()
				.set("name", "Torre de la Vela")
				.set("height", 26)
				.set("function", "Watchtower");

		Subject building = store.create("burj khalifa", "building");
		building.update()
				.set("name", "Burj khalifa")
				.set("year", 2010)
				.put("city", "Dubai")
				.put("country", "United Arab Emirates")
				.put("continent", "Asia");

		building.create("spire", "detail").update()
				.set("height", 829.8)
				.set("function", "Aesthetic and communication");

		building.create("observation deck", "detail").update()
				.set("name", "At the Top")
				.set("floor", 148)
				.set("height", 555);
	}


	private int toNumber(String value) {
		return Integer.parseInt(value);
	}
}
