package tests;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectStore;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@SuppressWarnings({"ResultOfMethodCallIgnored", "NewClassNamingConvention"})
public class SubjectStore_ {
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
	public void should_create_and_persist_subjects_with_hierarchy_and_history() throws IOException {
		File index = new File("index.triples");
		File journal = new File(index.getAbsolutePath() + ".journal");
		try {
			SubjectStore store = new SubjectStore(index);
			assertThat(store.has("taj-mahal", "building")).isFalse();
			assertThat(store.has("taj-mahal.building")).isFalse();
			assertThat(index.exists()).isFalse();
			assertThat(journal.exists()).isFalse();
			createSubjects(store);
			assertThat(index.exists()).isFalse();
			assertThat(journal.exists()).isTrue();
			test1(store);
			assertThat(store.has("taj_mahal.building/mina.detail")).isFalse();
			assertThat(store.has("taj_mahal.building/minaret.detail")).isTrue();
			assertThat(store.open("taj_mahal.building/minaret.detail").children().collect()).containsExactly(Subject.of("taj_mahal.building/minaret.detail/balcony.feature"));

			store.seal();
			assertThat(index.exists()).isTrue();
			assertThat(journal.exists()).isFalse();
			test2(store);
			store.open("taj_mahal", "building").drop();
			assertThat(index.exists()).isTrue();
			assertThat(journal.exists()).isTrue();
			assertThat(store.query().collect().size()).isEqualTo(6);
			assertThat(store.query().isRoot().size()).isEqualTo(2);
			assertThat(store.query().isType("building").collect().size()).isEqualTo(2);
			assertThat(store.query().isType("building").where("country").equals("Spain").collect().size()).isEqualTo(1);
			assertThat(store.query().isType("building").where("continent").equals("Asia").collect().size()).isEqualTo(1);
			assertThat(store.query().isType("building").isRoot().size()).isEqualTo(2);
			assertThat(store.query().isType("detail").collect().size()).isEqualTo(4);
			assertThat(store.query().isType("detail").isRoot().size()).isEqualTo(0);
			assertThat(store.query("root").size()).isEqualTo(2);
			assertThat(store.query("type:building").collect().size()).isEqualTo(2);
			assertThat(store.query("type:building where:country=Spain").collect().size()).isEqualTo(1);
			assertThat(store.query("type:building where:continent=Asia").collect().size()).isEqualTo(1);
			assertThat(store.query("type:building root").size()).isEqualTo(2);
			assertThat(store.query("type:detail").collect().size()).isEqualTo(4);
			assertThat(store.query("type:detail root").isRoot().size()).isEqualTo(0);
			assertThat(store.has("taj_mahal.building")).isFalse();
			assertThat(store.has("taj_mahal.building/minaret.detail")).isFalse();
			assertThat(store.has("taj_mahal.building/minaret.detail/balcony.feature")).isFalse();

			store.seal();
			assertThat(index.exists()).isTrue();
			assertThat(journal.exists()).isFalse();
			test3(store);
		}
		finally {
			index.delete();
			journal.delete();
		}
	}

	@Test
	public void should_create_histories() throws IOException {
		File index = new File("index.triples");
		try {
			SubjectStore store = new SubjectStore(index).connection(connection);
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
		Subject building = store.open("burj_khalifa", "building");

		assertThat(building.children().isType("detail").where("name").contains("Top").collect()).containsOnly(store.open("burj_khalifa.building/observation_deck.detail"));
		assertThat(building.children().isType("detail").where("name").contains("Top").collect()).containsOnly(store.open("burj_khalifa.building/observation_deck.detail"));
		assertThat(building.children().isType("detail").where("name").equals("At the Top").collect()).containsOnly(store.open("burj_khalifa.building/observation_deck.detail"));
		assertThat(building.children().isType("detail").where("name").equals("At the Top").isRoot().collect()).isEmpty();
		assertThat(building.children().isType("departament").collect().size()).isEqualTo(0);

		assertThat(store.query().collect().size()).isEqualTo(10);
		assertThat(store.query().isRoot().size()).isEqualTo(3);
		assertThat(store.query().isType("building").collect().size()).isEqualTo(3);
		assertThat(store.query().isType("building").where("country").equals("Spain").collect().size()).isEqualTo(1);
		assertThat(store.query().isType("building").where("continent").equals("Asia").collect().size()).isEqualTo(2);
		assertThat(store.query().isType("building").isRoot().size()).isEqualTo(3);
		assertThat(store.query().isType("detail").isRoot().size()).isEqualTo(0);
		assertThat(store.query().isType("detail").collect().size()).isEqualTo(6);
		assertThat(store.query("root").size()).isEqualTo(3);
		assertThat(store.query("type:building").collect().size()).isEqualTo(3);
		assertThat(store.query("type:building	 where:country=Spain").collect().size()).isEqualTo(1);
		assertThat(store.query("type:building where:continent=Asia").collect().size()).isEqualTo(2);
		assertThat(store.query("type:building root").size()).isEqualTo(3);
		assertThat(store.query("type:detail root").isRoot().size()).isEqualTo(0);
		assertThat(store.query("type:detail").collect().size()).isEqualTo(6);

	}

	private static void test2(SubjectStore store) {
		assertThat(store.has("alhambra", "building")).isTrue();
		assertThat(store.has("torre del oro", "building")).isFalse();
		assertThat(store.query().collect().size()).isEqualTo(10);
		assertThat(store.query().isRoot().size()).isEqualTo(3);
		assertThat(store.query().isType("building").collect().size()).isEqualTo(3);
		assertThat(store.query().isType("building").where("country").equals("Spain").collect().size()).isEqualTo(1);
		assertThat(store.query().isType("building").where("continent").equals("Asia").collect().size()).isEqualTo(2);
		assertThat(store.query().isType("building").isRoot().size()).isEqualTo(3);
		assertThat(store.query().isType("detail").collect().size()).isEqualTo(6);
		assertThat(store.query().isType("detail").isRoot().size()).isEqualTo(0);
		assertThat(store.query().isType("patient").collect().size()).isEqualTo(0);

		Subject building = store.query("type:building where:city=Dubai").first();
		assertThat(building.children().isType("detail").where("name").contains("Top").collect()).containsOnly(store.open("burj_khalifa.building/observation_deck.detail"));
		assertThat(building.children().isType("detail").where("name").contains("Top").collect()).containsOnly(store.open("burj_khalifa.building/observation_deck.detail"));
		assertThat(building.children().isType("detail").where("name").equals("At the Top").collect()).containsOnly(store.open("burj_khalifa.building/observation_deck.detail"));
		assertThat(building.children().isType("detail").where("name").equals("At the Top").isRoot().collect()).isEmpty();
		assertThat(building.children().isType("departament").collect().size()).isEqualTo(0);
	}

	private void test3(SubjectStore store) {
		assertThat(store.query().collect().size()).isEqualTo(6);
		assertThat(store.query().isRoot().size()).isEqualTo(2);
		assertThat(store.query().isType("building").collect().size()).isEqualTo(2);
		assertThat(store.query().isType("building").where("country").equals("Spain").collect().size()).isEqualTo(1);
		assertThat(store.query().isType("building").where("continent").contains("Asia").size()).isEqualTo(1);
		assertThat(store.query().isType("building").where("year").satisfy(v-> toNumber(v) > 1900).size()).isEqualTo(1);
		assertThat(store.query().isType("building").isRoot().size()).isEqualTo(2);
		assertThat(store.query().isType("detail").collect().size()).isEqualTo(4);
		assertThat(store.query().isType("detail").isRoot().size()).isEqualTo(0);
	}

	private void createHistory(SubjectStore store) {
		for (Subject subject : store.query().collect()) {
			SubjectHistory history = store.historyOf(subject);
			history.on("2025", "test").put("visits", 10).terminate();
		}
	}

	private void testHistory(SubjectStore store) {
		for (Subject subject : store.query().collect()) {
			SubjectHistory history = store.historyOf(subject);
			assertThat(history.query().number("visits").all().summary().count()).isEqualTo(1);
			assertThat(history.query().number("visits").all().summary().sum()).isEqualTo(10.0);
		}
	}




	private static void createSubjects(SubjectStore store) {
		Subject taj = store.create("taj-mahal", "building");
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
		Subject minaret = taj.create("mina", "detail");
		minaret.update()
				.set("count", 4)
				.set("height", 40);
		minaret.create("balcony", "feature").update()
				.set("diameter", 3.5)
				.set("position", "upper");
		minaret.rename("minaret");

		taj.rename("taj_mahal");


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

		Subject building = store.create("burj_khalifa", "building");
		building.update()
				.set("name", "Burj khalifa")
				.set("year", 2010)
				.put("city", "Dubai")
				.put("country", "United Arab Emirates")
				.put("continent", "Asia");

		building.create("spire", "detail").update()
				.set("height", 829.8)
				.set("function", "Aesthetic and communication");

		building.create("observation_deck", "detail").update()
				.set("name", "At the Top")
				.set("floor", 148)
				.set("height", 555);
	}


	private int toNumber(String value) {
		return Integer.parseInt(value);
	}
}
