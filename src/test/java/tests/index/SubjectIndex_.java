package tests.index;


import org.junit.Test;
import systems.intino.datamarts.subjectstore.model.Journal;
import systems.intino.datamarts.subjectstore.model.Term;
import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.parseInt;
import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.datamarts.subjectstore.SubjectQuery.OrderType.*;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndex_ {

	@Test
	public void should_create_and_rename_subject_and_preserve_terms_across_sessions() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		index.create("11", "o").update().put("name", "jose");
		assertThat(index.open("11", "o").terms()).containsOnly(terms("name=jose"));
		index.open("11", "o").rename("22");
		assertThat(index.open("11", "o")).isNull();
		assertThat(index.open("22", "o").terms()).containsOnly(terms("name=jose"));
		assertThat(Files.readString(file.toPath())).isEqualTo("put 11.o name=jose\nrename 11.o 22\n");
	}

	@Test
	public void should_allow_idempotent_subject_creation_and_support_queries_after_reload() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		index.create("11", "o").update().put("name", "jose");
		index.create("11", "o").update().put("name", "jose");
		assertThat(index.query().isRoot().first().toString()).isEqualTo("11.o");
		assertThat(index.query().isType("o").collect()).containsOnly(subject("11.o"));
		assertThat(index.query().isType("o").isRoot().first()).isEqualTo(subject("11.o"));
		assertThat(index.query().where("name").equals("jose").isRoot().first()).isEqualTo(subject("11.o"));
		assertThat(index.query().where("name").equals("jose").isType("o").first()).isEqualTo(subject("11.o"));
		assertThat(index.query().where("name").equals("jose").isType("o").isRoot().first()).isEqualTo(subject("11.o"));
		assertThat(index.query().isType("p").isEmpty()).isTrue();
		assertThat(index.query().isType("p").isRoot().isEmpty()).isTrue();
		assertThat(index.query().isType("p").where("name").equals("jose").isEmpty()).isTrue();
		assertThat(Files.readString(file.toPath())).isEqualTo("put 11.o name=jose\n");
	}

	@Test
	public void should_support_full_term_lifecycle_with_persistence() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		index.create("11", "o").update().set("name", "jose");
		assertThat(index.open("11","o").terms()).containsOnly(terms("name=jose"));
		Subject subject = index.open("11", "o");
		subject.update().set("name", "mario");
		assertThat(subject.terms()).containsOnly(terms("name=mario"));
		subject.update().set("name", "maria");
		assertThat(subject.terms()).containsOnly(terms("name=maria"));
		subject.update().del("name");
		assertThat(subject.terms()).isEmpty();
		subject.update().put("name", "pablo");
		subject.update().put("name", "pedro");
		assertThat(subject.terms()).containsOnly(terms("name=pablo","name=pedro"));
		subject.update().del("name");
		assertThat(subject.terms()).isEmpty();
		assertThat(Files.readString(file.toPath())).isEqualTo("""
			set 11.o name=jose
			set 11.o name=mario
			set 11.o name=maria
			del 11.o name=maria
			put 11.o name=pablo
			put 11.o name=pedro
			del 11.o name=pablo
			del 11.o name=pedro
			""");
	}

	@Test
	public void should_handle_special_characters_and_export_cleanly() throws IOException {
		byte[] bytes;
		{
			File file = File.createTempFile("index", ".journal");
			SubjectIndex index = new SubjectIndex(file);
			index.create("11", "o").update().set("name", "jose\tjuan");
			index.create("11", "o").update().set("info", "this is\na description");
			assertThat(index.open("11", "o").terms()).containsOnly(terms("name=jose\tjuan", "info=this is\na description"));
			Subject subject = index.open("11", "o");
			subject.update().set("info", "");
			assertThat(subject.terms()).containsOnly(terms("name=jose\tjuan"));
			subject.update().put("memo", "\na\nb\n");
			assertThat(Files.readString(file.toPath())).isEqualTo("""
					set 11.o name=jose␉juan
					set 11.o info=this is␤a description
					del 11.o info=this is␤a description
					put 11.o memo=a␤b
					""");
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			index.dump(os);
			bytes = os.toByteArray();
			assertThat(new String(bytes).trim()).isEqualTo("11.o\tname\tjose␉juan\n11.o\tmemo\ta␤b");
		}
		{
			SubjectIndex index = new SubjectIndex(new File("")).restore(new ByteArrayInputStream(bytes));
			Subject subject = index.open("11", "o");
			subject.update().set("info", "");
			assertThat(subject.terms()).containsOnly(terms("name=jose\tjuan", "memo=a␤b"));
		}
	}

	@Test
	public void should_support_restore_and_update_journal() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file).restore(triples("theaters.triples"));
		assertThat(index.query().isRoot().collect()).containsOnly(subject("181.theater"),subject("182.theater"),subject("383.theater"));
		assertThat(index.query().where("class").equals("2D").size()).isEqualTo(15);
		assertThat(index.query().size()).isEqualTo(31);
		assertThat(index.query().where("seats").satisfy(v -> parseInt(v) >= 300).size()).isEqualTo(2);
		assertThat(index.query().isType("screen").where("seats").satisfy(v -> parseInt(v) >= 200).size()).isEqualTo(4);
		index.open("181.theater").drop();
		assertThat(index.query().isRoot().collect()).containsOnly(subject("182.theater"),subject("383.theater"));
		assertThat(index.query().where("class").equals("2D").size()).isEqualTo(7);
		assertThat(index.query().size()).isEqualTo(15);
		assertThat(index.query().isType("screen").size()).isEqualTo(13);
		assertThat(index.query().where("seats").satisfy(v -> parseInt(v) >= 300).size()).isEqualTo(2);
		assertThat(index.query().where("seats").satisfy(v -> parseInt(v) >= 200).size()).isEqualTo(3);
		assertThat(Files.readString(file.toPath())).isEqualTo("drop 181.theater -\n");
	}

	@Test
	public void should_navigate_and_query_subject_hierarchy_correctly() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		Subject s1 = index.create("1","o");
		s1.update().put("class", "a");
		Subject s12 = s1.create("12", "p");
		s12.update().put("class", "x");
		Subject s123 = s12.create("123", "q");
		s123.update().put("class", "m");
		Subject s124 = s12.create("124", "q");
		s124.update().put("class", "m");
		s124.drop();

		assertThat(index.query().isRoot().collect().size()).isEqualTo(1);
		assertThat(index.query().collect().size()).isEqualTo(3);
		assertThat(index.query().isType("o").collect().size()).isEqualTo(1);
		assertThat(index.query().isType("p").collect().size()).isEqualTo(1);
		assertThat(index.query().isType("p").first().identifier()).isEqualTo("1.o/12.p");
		assertThat(index.query().isRoot().first()).isEqualTo(Subject.of("1.o"));
		assertThat(index.query().isRoot().first().children().first()).isEqualTo(Subject.of("1.o/12.p"));
		assertThat(index.open("1.o").isNull()).isFalse();
		assertThat(index.open("1","o").parent().isNull()).isTrue();
		assertThat(index.open("2" ,"o")).isNull();
		assertThat(index.open("2" ,"p")).isNull();
		assertThat(index.open("1.o").open("12.p").identifier()).isEqualTo("1.o/12.p");
		assertThat(index.open("1.o").open("12","p").identifier()).isEqualTo("1.o/12.p");
		assertThat(index.open("1.o").open("12.p").open("123.q").identifier()).isEqualTo("1.o/12.p/123.q");
		assertThat(index.open("1.o").open("12.p/123.q").identifier()).isEqualTo("1.o/12.p/123.q");
		assertThat(index.open("1.o/12.p").parent()).isEqualTo(index.open("1","o"));
		assertThat(index.open("1.o/12.p").children().collect()).containsOnly(Subject.of("1.o/12.p/123.q"));
		assertThat(index.open("1.o/12.p").children().first().identifier()).isEqualTo("1.o/12.p/123.q");
		assertThat(index.open("1.o/12.p/123.q").parent().parent()).isEqualTo(Subject.of("1.o"));
		assertThat(index.query().where("class").equals("a").isRoot().collect().size()).isEqualTo(1);
		assertThat(index.query().isType("o").where("class").equals("a").isRoot().collect().size()).isEqualTo(1);
		assertThat(index.query().isType("p").where("class").equals("a").isRoot().collect().size()).isEqualTo(0);
		assertThat(index.query().isType("p").where("class").equals("x").collect().size()).isEqualTo(1);
		assertThat(index.query().isType("p").where("class").equals("x").first()).isEqualTo(Subject.of("1.o/12.p"));
		assertThat(index.query().where("class").equals("x").isRoot().collect().isEmpty()).isTrue();
		assertThat(Files.readString(file.toPath())).isEqualTo("""
			put 1.o class=a
			put 1.o/12.p class=x
			put 1.o/12.p/123.q class=m
			put 1.o/12.p/124.q class=m
			drop 1.o/12.p/124.q -
			""");
	}

	@Test
	public void should_support_sorting() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file).restore(triples("theaters.triples"));
		assertThat(index.query().isType("theater").orderBy("email").collect()).containsExactly(subject("182.theater"), subject("181.theater"),subject("383.theater"));
		assertThat(index.query("type:theater").orderBy("email").collect()).containsExactly(subject("182.theater"), subject("181.theater"),subject("383.theater"));

		assertThat(index.query("type:theater order:email").collect()).containsExactly(subject("182.theater"), subject("181.theater"),subject("383.theater"));
		assertThat(index.query("type:theater order:email?asc").collect()).containsExactly(subject("182.theater"), subject("181.theater"),subject("383.theater"));
		assertThat(index.query("type:theater order:email?desc").collect()).containsExactly(subject("383.theater"), subject("181.theater"),subject("182.theater"));
		assertThat(index.query("type:theater order:email?text&desc").collect()).containsExactly(subject("383.theater"), subject("181.theater"),subject("182.theater"));

		assertThat(index.query().isType("theater").orderBy("name").collect()).containsOnly(subject("383.theater"), subject("182.theater"),subject("181.theater"));
		assertThat(index.query("type:theater order:name?asc").collect()).containsOnly(subject("383.theater"), subject("182.theater"),subject("181.theater"));
		assertThat(index.query("type:theater order:name?desc").collect()).containsOnly(subject("181.theater"), subject("182.theater"),subject("383.theater"));
		assertThat(index.query().isType("screen").orderBy("seats", NumericAscending).first()).isEqualTo(subject("181.theater/11.screen"));
		assertThat(index.query("type:screen order:seats?num&asc").first()).isEqualTo(subject("181.theater/11.screen"));
		assertThat(index.query("type:screen order:seats?asc&num").first()).isEqualTo(subject("181.theater/11.screen"));
		assertThat(index.query("type:screen order:seats?desc&num").first()).isEqualTo(subject("383.theater/3.screen"));
		assertThat(index.query().isType("screen").where("class").equals("2D").orderBy("seats", NumericDescending).first()).isEqualTo(subject("383.theater/5.screen"));
		assertThat(index.query("type:screen where:class=2D order:seats?num&desc").first()).isEqualTo(subject("383.theater/5.screen"));
		assertThat(Files.readString(file.toPath())).isEqualTo("");
	}

	@Test
	public void should_support_del_terms_and_conditional_query() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		index.create("123456", "model").update()
				.put("description","simulation")
				.put("user", "mcaballero@gmail.com");
		index.create("654321","model").update()
				.put("t","simulation")
				.put("user", "mcaballero@gmail.com")
				.put("user", "josejuan@gmail.com");
		index.create("654321","model").update()
				.del("t","simulation")
				.del("t","simulation");

		assertThat(index.query().isRoot().collect()).containsOnly(subject("123456.model"), subject("654321.model"));
		assertThat(index.query().isRoot().collect()).containsOnly(subject("123456.model"), subject("654321.model"));
		assertThat(index.query().where("user").equals("mcaballero@gmail.com").isRoot().collect()).containsOnly(subject("123456.model"), subject("654321.model"));
		assertThat(index.query().where("description").equals("simulation").isRoot().collect()).containsOnly(subject("123456.model"));
		Subject result = Subject.of("654321.model");
		assertThat(index.query().where("user").equals("josejuan@gmail.com").isRoot().collect()).containsOnly(result);
		assertThat(Files.readString(file.toPath())).isEqualTo("""
				put 123456.model description=simulation
				put 123456.model user=mcaballero@gmail.com
				put 654321.model t=simulation
				put 654321.model user=mcaballero@gmail.com
				put 654321.model user=josejuan@gmail.com
				del 654321.model t=simulation
				""");
	}

	@Test
	public void should_open_terms_of_subject() throws Exception {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		index.create("11", "o").update().put("name", "jose");
		index.create("123456", "model").update()
				.put("t", "simulation")
				.put("user", "mcaballero@gmail.com")
				.put("user", "josejuan@gmail.com")
				.put("project", "ulpgc")
				.put("t", "simulation")
				.put("team", "ulpgc");
		index.create("123456", "model").update()
				.del("team", "ulpgc");
		assertThat(index.open("11","o").terms()).containsOnly(new Term("name","jose"));
		assertThat(index.open("123456", "model").terms()).containsOnly(terms("t=simulation","user=mcaballero@gmail.com","user=josejuan@gmail.com","project=ulpgc"));
		assertThat(index.open("123456", "model").has("t")).isTrue();
		assertThat(index.open("123456", "model").has("simulation")).isFalse();
		assertThat(index.open("123456", "model").has("user")).isTrue();
		assertThat(Files.readString(file.toPath())).isEqualTo("""
				put 11.o name=jose
				put 123456.model t=simulation
				put 123456.model user=mcaballero@gmail.com
				put 123456.model user=josejuan@gmail.com
				put 123456.model project=ulpgc
				put 123456.model team=ulpgc
				del 123456.model team=ulpgc
				""");
	}

	@Test
	public void should_search_using_contain_and_fit_filters() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		index.create("123456", "model").update()
				.put("description", "simulation")
				.put("user", "josejuan@gmail.com")
				.put("project", "ulpgc")
				.put("description", "simulation")
				.put("team", "ulpgc.es")
				.put("access", ".*@ulpgc\\.es");
		index.create("654321","model").update()
				.put("description", "simulation")
				.put("user", "mcaballero@gmail.com")
				.put("project", "ulpgc");
		index.create("123456", "model").update().del("team", "ulpgc");
		assertThat(index.query().where("project").equals("ulpgc").collect()).containsOnly(Subject.of("654321.model"), Subject.of("123456.model"));
		assertThat(index.query().where("team").equals("ulpgc.es").collect()).containsOnly(Subject.of("123456.model"));
		assertThat(index.query().where("team").contains("ulpgc").collect()).containsOnly(Subject.of("123456.model"));
		assertThat(index.query().where("team").contains("ulpgc", "es").collect()).containsOnly(Subject.of("123456.model"));
		assertThat(index.query().where("description").contains("sim").collect()).containsOnly(Subject.of("123456.model"), Subject.of("654321.model"));
		assertThat(index.query().where("description").contains("xxx").collect()).isEmpty();
		assertThat(index.query().where("access").accepts("jose@gmail.com").collect()).isEmpty();
		assertThat(index.query().where("access").accepts("jose@ulpgc.es").collect()).containsOnly(Subject.of("123456.model"));
		assertThat(Files.readString(file.toPath())).isEqualTo("""
				put 123456.model description=simulation
				put 123456.model user=josejuan@gmail.com
				put 123456.model project=ulpgc
				put 123456.model team=ulpgc.es
				put 123456.model access=.*@ulpgc\\.es
				put 654321.model description=simulation
				put 654321.model user=mcaballero@gmail.com
				put 654321.model project=ulpgc
				""");
	}

	@Test
	public void should_index_query_with_terms_and_support_deletion() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		index.create("P001.model").update()
				.put("name", "AI Research")
				.put("lead", "alice@example.com")
				.put("status", "active");
		index.create("P001.model/E1.experiment").update()
				.put("name", "Language Model Evaluation")
				.put("dataset", "OpenQA");
		index.create("P001.model/E2.experiment").update()
				.put("name", "Graph Alignment")
				.put("dataset", "wikipedia")
				.put("status", "archived");
		index.create("P001.model/E2.experiment").update().del("status", "archived");
		index.open("P001.model").children().first().rename("E001");
		index.open("P001.model").children().collect().get(1).rename("E002");
		index.open("P001.model/E001.experiment").drop();
		assertThat(index.open("P001.model").isNull()).isFalse();
		assertThat(index.open("P001.model/E001.experiment")).isNull();
		assertThat(index.open("P001.model/E002.experiment").terms()).doesNotContain(terms("status=archived"));
		assertThat(index.open("P001.model").children().collect()).hasSize(1);
		assertThat(index.open("P001.model").children().first().name()).isEqualTo("E002");
		assertThat(index.query().isType("model").where("name").equals("AI Research").collect()).contains(Subject.of("P001.model"));
		assertThat(Files.readString(file.toPath())).isEqualTo("""
				put P001.model name=AI Research
				put P001.model lead=alice@example.com
				put P001.model status=active
				put P001.model/E1.experiment name=Language Model Evaluation
				put P001.model/E1.experiment dataset=OpenQA
				put P001.model/E2.experiment name=Graph Alignment
				put P001.model/E2.experiment dataset=wikipedia
				put P001.model/E2.experiment status=archived
				del P001.model/E2.experiment status=archived
				rename P001.model/E1.experiment E001
				rename P001.model/E2.experiment E002
				drop P001.model/E001.experiment -
				""");
	}

	@Test
	public void should_operate_subject_sequences() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file).restore(triples("subjects.triples"));
		assertThat(index.open("P001.model").next("#component")).isEqualTo("1");
		assertThat(index.open("P001.model").next("#component")).isEqualTo("2");
	}

	@Test
	public void should_get_and_set_term_subjects() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file).restore(triples("subjects.triples"));
		index.open("P001.model").update()
				.set("main", index.open("P002.model"));
		assertThat(index.open("P001.model").has("main")).isTrue();
		assertThat(index.open("P001.model").get("main")).isEqualTo(index.open("P002.model").identifier());
		index.open("P001.model").update()
				.del("main", index.open("P002.model"))
				.put("related", index.open("P003.model"))
				.put("related", index.open("P004.model"));
		assertThat(index.open("P001.model").has("main")).isFalse();
		assertThat(index.open("P001.model").has("related")).isTrue();
		assertThat(index.open("P001.model").get("related")).isEqualTo("P003.model, P004.model");
	}

	@Test
	public void should_restore_from_triples() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file).restore(triples("subjects.triples"));
		index.open("P001.model").update().set("status", "running").set("lead", "alice@example.com");
		index.open("P002.model").update().del("lead");
		index.open("P003.model").update().del("lead").del("status");
		index.open("P003.model").update().set("lead","mary@example.com").set("status", "running");
		index.open("P004.model").update().del("lead").del("status");
		index.open("P004.model").update().del("lead").del("status");

		assertThat(index.query().collect().size()).isEqualTo(125);
		assertThat(index.open("P001.model").terms()).contains(new Term("status","running"));
		assertThat(index.open("P001.model").terms()).doesNotContain(new Term("lead","user1@example.com"));
		assertThat(index.open("P001.model").terms()).contains(new Term("lead","alice@example.com"));
		assertThat(index.open("P002.model").terms()).containsOnly(new Term("status","active"),new Term("name","Project 2"));
		assertThat(index.open("P003.model").terms()).containsOnly(new Term("status","running"), new Term("lead","mary@example.com"), new Term("name","Project 3"));
		assertThat(index.open("P004.model").terms()).containsOnly(new Term("name","Project 4"));
		assertThat(index.query().collect().size()).isEqualTo(125);
		assertThat(index.open("P001.model").terms()).contains(new Term("status","running"));
		assertThat(index.open("P001.model").terms()).doesNotContain(new Term("lead","user1@example.com"));
		assertThat(index.open("P001.model").terms()).contains(new Term("lead","alice@example.com"));
		assertThat(index.open("P002.model").terms()).contains(new Term("status","active"));
		assertThat(index.open("P003.model").terms()).containsOnly(new Term("status","running"), new Term("lead","mary@example.com"), new Term("name","Project 3"));
		assertThat(index.open("P004.model").terms()).containsOnly(new Term("name","Project 4"));
		assertThat(Files.readString(file.toPath())).isEqualTo("""
				set P001.model status=running
				set P001.model lead=alice@example.com
				del P002.model lead=user2@example.com
				del P003.model lead=user3@example.com
				del P003.model status=active
				set P003.model lead=mary@example.com
				set P003.model status=running
				del P004.model lead=user4@example.com
				del P004.model status=active
				""");
	}

	@SuppressWarnings("resource")
	@Test
	public void should_dump_index() throws Exception {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file).restore(triples("movies.triples"));
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		index.dump(os);
		String string = new String(triples("movies.triples").readAllBytes()).replace("\r", "");
		assertThat(os.toString()).isEqualTo(string);
	}

	@Test
	public void should_detect_deletions() throws IOException {
		File file = File.createTempFile("index",".journal");
		SubjectIndex index = new SubjectIndex(file);
		Subject subject = index.create("P001.model");
		subject.update()
				.put("name", "AI Research")
				.put("lead", "alice@example.com");

		Journal journal = Journal.from(subject).to(List.of());
		String expected = String.join("\n",
				"del P001.model name=AI Research",
				"del P001.model lead=alice@example.com"
		);
		assertThat(journal.toString()).isEqualTo(expected);
	}

	@Test
	public void should_detect_insertions() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		Subject subject = index.create("P001.model");
		List<Term> terms = List.of(
				new Term("name", "AI Research"),
				new Term("lead", "alice@example.com")
		);

		Journal journal = Journal.from(subject).to(terms);
		String expected = String.join("\n",
				"put P001.model name=AI Research",
				"put P001.model lead=alice@example.com"
		);
		assertThat(journal.toString()).isEqualTo(expected);
	}

	@Test
	public void should_detect_no_change() throws IOException {
		File file = tempFile();
		SubjectIndex index = new SubjectIndex(file);
		Subject subject = index.create("P001.model");
		subject.update()
				.put("name", "AI Research")
				.put("lead", "alice@example.com");

		List<Term> terms = List.of(
				new Term("name", "AI Research"),
				new Term("lead", "alice@example.com")
		);

		Journal journal = Journal.from(subject).to(terms);
		assertThat(journal.toString()).isEqualTo("");
	}

	private Term[] terms(String... strings) {
		return Arrays.stream(strings).map(s->term(s.split("="))).toArray(Term[]::new);
	}

	private static Term term(String[] s) {
		return new Term(s[0], s[1]);
	}

	private static Subject subject(String s) {
		return Subject.of(s);
	}

	private static InputStream triples(String name) {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream(name);
	}

	private static File tempFile() throws IOException {
		File file = File.createTempFile("index",".journal");
		file.deleteOnExit();
		return file;
	}


}
