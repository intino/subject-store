package tests;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.SubjectHistoryView;
import systems.intino.datamarts.subjectstore.SubjectIndexView;
import systems.intino.datamarts.subjectstore.SubjectStore;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectStore_ {
	@Test
	public void should_create_and_persist_subjects_with_hierarchy_and_history() throws IOException {
		File file = File.createTempFile("xxx", ".iss");
		try (SubjectStore store = new SubjectStore(Storages.in(file))) {
			assertThat(store.has("taj-mahal", "building")).isFalse();

			createSubjects(store);
			Subject building = store.get("burj khalifa", "building");

			assertThat(building.children("detail").where("name").contains("Top")).containsExactly(store.get("burj khalifa.building/observation deck.detail"));
			assertThat(building.children("detail").where("name").contains("top")).containsExactly(store.get("burj khalifa.building/observation deck.detail"));
			assertThat(building.children("detail").with("name", "At the Top").collect()).containsExactly(store.get("burj khalifa.building/observation deck.detail"));
			assertThat(building.children("detail").with("name", "At the Top").isRoot().collect()).isEmpty();
			assertThat(building.children("detail").that(s1 -> s1.children().size() == 0).collect().size()).isEqualTo(2);
			assertThat(building.children("detail").that(s1 -> s1.children().size() > 0).collect().size()).isEqualTo(0);
			assertThat(building.children("departament").collect().size()).isEqualTo(0);

			assertThat(store.subjects().collect().size()).isEqualTo(9);
			assertThat(store.subjects().roots().size()).isEqualTo(3);
			assertThat(store.subjects("building").collect().size()).isEqualTo(3);
			assertThat(store.subjects("building").with("country", "Spain").collect().size()).isEqualTo(1);
			assertThat(store.subjects("building").with("continent", "Asia").collect().size()).isEqualTo(2);
			assertThat(store.subjects("building").roots().size()).isEqualTo(3);
			assertThat(store.subjects("detail").roots().size()).isEqualTo(0);
			assertThat(store.subjects("detail").collect().size()).isEqualTo(6);
			assertThat(store.subjects("patient").collect().size()).isEqualTo(0);
		}

		try (SubjectStore store = new SubjectStore(Storages.in(file))) {
			assertThat(store.has("alhambra", "building")).isTrue();
			assertThat(store.has("torre del oro", "building")).isFalse();
			assertThat(store.subjects().collect().size()).isEqualTo(9);
			assertThat(store.subjects().roots().size()).isEqualTo(3);
			assertThat(store.subjects("building").collect().size()).isEqualTo(3);
			assertThat(store.subjects("building").with("country", "Spain").collect().size()).isEqualTo(1);
			assertThat(store.subjects("building").with("continent", "Asia").collect().size()).isEqualTo(2);
			assertThat(store.subjects("building").roots().size()).isEqualTo(3);
			assertThat(store.subjects("detail").collect().size()).isEqualTo(6);
			assertThat(store.subjects("detail").roots().size()).isEqualTo(0);
			assertThat(store.subjects("patient").collect().size()).isEqualTo(0);

			Subject building = store.subjects("building").with("city","Dubai").first();
			assertThat(building.children("detail").where("name").contains("Top")).containsExactly(store.get("burj khalifa.building/observation deck.detail"));
			assertThat(building.children("detail").where("name").contains("top")).containsExactly(store.get("burj khalifa.building/observation deck.detail"));
			assertThat(building.children("detail").with("name", "At the Top").collect()).containsExactly(store.get("burj khalifa.building/observation deck.detail"));
			assertThat(building.children("detail").with("name", "At the Top").isRoot().collect()).isEmpty();
			assertThat(building.children("detail").that(s1 -> s1.children().size() == 0).collect().size()).isEqualTo(2);
			assertThat(building.children("detail").that(s1 -> s1.children().size() > 0).collect().size()).isEqualTo(0);
			assertThat(building.children("departament").collect().size()).isEqualTo(0);

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
			assertThat(store.subjects("building").where("year").that(v-> toNumber(v) > 1900).size()).isEqualTo(1);
			assertThat(store.subjects("building").roots().size()).isEqualTo(2);
			assertThat(store.subjects("detail").collect().size()).isEqualTo(4);
			assertThat(store.subjects("detail").roots().size()).isEqualTo(0);
		}
	}

	@Test
	public void should_create_views() throws IOException {
		File file = File.createTempFile("xxx", ".iss");
		try (SubjectStore store = new SubjectStore(Storages.in(file))) {
			createSubjects(store);
			SubjectIndexView view = store.view()
					.type("building")
					.add("year")
					.add("city")
					.add("country")
					.build();
			SubjectHistoryView tajMahalHistory = store
					.viewOf("taj mahal.building")
					.with(new File("format.yaml"));
			SubjectHistoryView historyView = store
					.viewOf("taj mahal.building")
					.from("2010")
					.to("2025-04")
					.duration("P1Y")
					.add("","")
					.build();
			assertThat(view.column("city").summary().categories()).containsExactly("Agra", "Granada", "Dubai");
			assertThat(view.column("city").summary().frequency("Agra")).isEqualTo(1);
			assertThat(view.column("country").summary().categories()).containsExactly("United Arab Emirates", "Spain", "India");
		}
	}

	private static void createSubjects(SubjectStore store) {
		Subject s = store.create("taj-mahal", "building");
		Subject taj = s.rename("taj mahal");
		taj.index()
				.set("name", "taj mahal")
				.set("year", 1648)
				.put("city", "Agra")
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


		Subject alhambra = store.create("alhambra", "building");
		alhambra.index()
				.set("name", "Alhambra")
				.set("year", 889)
				.put("city", "Granada")
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
				.set("function", "Watchtower")
				.terminate();

		Subject building = store.create("burj khalifa", "building");
		building.index()
				.set("name", "Burj khalifa")
				.set("year", 2010)
				.put("city", "Dubai")
				.put("country", "United Arab Emirates")
				.put("continent", "Asia")
				.terminate();

		building.create("spire", "detail").index()
				.set("height", 829.8)
				.set("function", "Aesthetic and communication")
				.terminate();

		building.create("observation deck", "detail").index()
				.set("name", "At the Top")
				.set("floor", 148)
				.set("height", 555)
				.terminate();
	}

	private int toNumber(String value) {
		return Integer.parseInt(value);
	}
}
