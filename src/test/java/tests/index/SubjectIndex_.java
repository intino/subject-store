package tests.index;


import org.junit.Test;
import systems.intino.datamarts.subjectstore.model.Term;
import tests.Storages;
import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndex_ {

	@Test
	public void should_support_index_subject_and_conditional_query() throws Exception {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("11", "o").index().put("name", "jose").terminate();
			check(index);
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("11", "o").index()
					.put("name", "jose")
					.terminate();
			check(index);
		}
	}

	private static void check(SubjectIndex index)  {
		assertThat(index.subjects().isRoot().first().toString()).isEqualTo("11.o");
		assertThat(index.subjects().type("o").isRoot().first().toString()).isEqualTo("11.o");
		assertThat(index.subjects().type("p").isRoot().isEmpty()).isTrue();
		assertThat(index.subjects().with("name", "jose").isRoot().first().toString()).isEqualTo("11.o");
		assertThat(index.subjects().without("name", "jose").isRoot().isEmpty()).isTrue();
		assertThat(index.subjects().without("name", "mario").isRoot().first().toString()).isEqualTo("11.o");
	}

	@Test
	public void should_support_rename_query() throws Exception {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("11", "o").index().put("name", "jose").terminate();
			index.open("11", "o").rename("22");
			assertThat(index.open("11", "o")).isNull();
			assertThat(index.open("22", "o").terms()).containsExactly(terms("name=jose"));
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			assertThat(index.open("11","o")).isNull();
			assertThat(index.open("22", "o").terms()).containsExactly(terms("name=jose"));
		}
	}

	@Test
	public void should_support_set_and_del_terms() throws Exception {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("11", "o").index().set("name", "jose").terminate();
			assertThat(index.open("11","o").terms()).containsExactly(terms("name=jose"));
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.open("11", "o");
			assertThat(subject.terms()).containsExactly(terms("name=jose"));
			subject.index().set("name", "mario").terminate();
			assertThat(subject.terms()).containsExactly(terms("name=mario"));
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.open("11", "o");
			assertThat(subject.terms()).containsExactly(terms("name=mario"));
			subject.index().del("name").terminate();
			assertThat(subject.terms()).isEmpty();
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.open("11", "o");
			assertThat(subject.terms()).isEmpty();
			subject.index().put("name", "mario").terminate();
			subject.index().put("name", "jose").terminate();
			assertThat(subject.terms()).containsExactly(terms("name=mario\nname=jose"));
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.open("11", "o");
			assertThat(subject.terms()).containsExactly(terms("name=mario\nname=jose"));
			subject.index().del("name").terminate();
			assertThat(subject.terms()).isEmpty();
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.open("11", "o");
			assertThat(subject.terms()).isEmpty();
		}
	}

	@Test
	public void should_navigate_subject_structure() throws Exception {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject s1 = index.create("1","o");
			s1.index().put("value", "1").terminate();
			Subject s2 = s1.create("12", "p");
			s2.index().put("value", "2").terminate();
			Subject s3 = s2.create("123", "q");
			s3.index().put("value", "3").terminate();
			Subject s4 = s2.create("124", "q");
			s4.index().put("value", "4").terminate();
			s4.drop();

			index.subjects()
					.with("visibility", "private")
					.with("owner", "34102340134112341")
					.without("usage", "template")
					.collect();

			assertThat(index.subjects().isRoot().collect().size()).isEqualTo(1);
			assertThat(index.subjects().collect().size()).isEqualTo(3);
			assertThat(index.subjects().type("o").collect().size()).isEqualTo(1);
			assertThat(index.subjects().type("p").collect().size()).isEqualTo(1);
			assertThat(index.subjects().type("p").first().identifier()).isEqualTo("1.o/12.p");
			assertThat(index.subjects().isRoot().first()).isEqualTo(Subject.of("1.o"));
			assertThat(index.subjects().isRoot().first().children().first()).isEqualTo(Subject.of("1.o/12.p"));
			assertThat(index.open("1.o").isNull()).isFalse();
			assertThat(index.open("1","o").parent().isNull()).isTrue();
			assertThat(index.open("2" ,"o")).isNull();
			assertThat(index.open("2" ,"p")).isNull();
			assertThat(index.open("1.o").open("12.p").identifier()).isEqualTo("1.o/12.p");
			assertThat(index.open("1.o").open("12","p").identifier()).isEqualTo("1.o/12.p");
			assertThat(index.open("1.o").open("12.p").open("123.q").identifier()).isEqualTo("1.o/12.p/123.q");
			assertThat(index.open("1.o").open("12.p/123.q").identifier()).isEqualTo("1.o/12.p/123.q");
			assertThat(index.open("1.o/12.p").parent()).isEqualTo(index.open("1","o"));
			assertThat(index.open("1.o/12.p").children().collect()).containsExactly(index.open("1.o/12.p/123.q"));
			assertThat(index.open("1.o/12.p").children().first().identifier()).isEqualTo("1.o/12.p/123.q");
			assertThat(index.open("1.o/12.p/123.q").parent().parent()).isEqualTo(Subject.of("1.o"));
			assertThat(index.subjects().with("value", "1").isRoot().collect().size()).isEqualTo(1);
			assertThat(index.subjects().type("o").with("value", "1").isRoot().collect().size()).isEqualTo(1);
			assertThat(index.subjects().type("p").with("value", "1").isRoot().collect().size()).isEqualTo(0);
			assertThat(index.subjects().type("p").with("value", "2").collect().size()).isEqualTo(1);
			assertThat(index.subjects().type("p").with("value", "2").first()).isEqualTo(Subject.of("1.o/12.p"));
			assertThat(index.subjects().with("value", "2").isRoot().collect().isEmpty()).isTrue();
		}
	}

	@Test
	public void should_support_drop_query() throws Exception {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.create("11", "o");
			subject.index()
					.put("name", "jose")
					.put("team", "first")
					.terminate();
			index.create("22","o").index()
					.put("name", "luis")
					.put("team", "first")
					.terminate();
			index.open("11.o").drop();
			assertThat(index.subjects().isRoot().first().toString()).isEqualTo("22.o");
			assertThat(index.terms()).containsExactly(new Term("team","first"), new Term("name","luis"));
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			assertThat(index.subjects().isRoot().first().toString()).isEqualTo("22.o");
		}
	}

	@Test
	public void should_support_del_terms_and_conditional_query() throws Exception {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("123456", "model").index()
					.put("description","simulation")
					.put("user", "mcaballero@gmail.com")
					.terminate();
			index.create("654321","model").index()
					.put("t","simulation")
					.put("user", "mcaballero@gmail.com")
					.put("user", "josejuan@gmail.com")
					.terminate();
			assertThat(index.terms()).containsExactly(terms("description=simulation\nuser=mcaballero@gmail.com\nt=simulation\nuser=josejuan@gmail.com"));
			index.create("654321","model").index()
					.del("t","simulation")
					.terminate();
			assertThat(index.terms()).containsExactly(terms("description=simulation\nuser=mcaballero@gmail.com\nuser=josejuan@gmail.com"));
			assertThat(index.subjects().isRoot().collect()).containsExactly(subjects("123456.model\n654321.model"));
			assertThat(index.subjects().isRoot().collect()).containsExactly(subjects("123456.model\n654321.model"));
			assertThat(index.subjects().with("user","mcaballero@gmail.com").isRoot().collect()).containsExactly(subjects("123456.model\n654321.model"));
			assertThat(index.subjects().with("description","simulation").isRoot().collect()).containsExactly(subjects("123456.model"));
			assertThat(index.subjects().with("user", "josejuan@gmail.com").isRoot().collect()).containsExactly(subjects("654321.model"));
			assertThat(index.subjects().without("description","simulation").isRoot().collect()).containsExactly(subjects("654321.model"));
			assertThat(index.subjects().without("user", "josejuan@gmail.com").isRoot().collect()).containsExactly(subjects("123456.model"));
		}
	}

	private Subject[] subjects(String str) {
		if (str.isEmpty()) return new Subject[0];
		return Arrays.stream(str.split("\n")).map(this::subject).toArray(Subject[]::new);
	}

	private Term[] terms(String str) {
		if (str.isEmpty()) return new Term[0];
		return Arrays.stream(str.split("\n")).map(s->term(s.split("="))).toArray(Term[]::new);
	}

	private Term term(String[] s) {
		return new Term(s[0], s[1]);
	}
	private Subject subject(String s) {
		return new Subject(s);
	}

	@Test
	public void should_open_terms_of_subject() throws Exception {
		File file = File.createTempFile("file", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("11", "o").index()
					.put("name", "jose")
					.terminate();
			index.create("123456", "model").index()
					.put("t", "simulation")
					.put("user", "mcaballero@gmail.com")
					.put("user", "josejuan@gmail.com")
					.put("project", "ulpgc")
					.put("t", "simulation")
					.put("team", "ulpgc")
					.terminate();
			index.create("123456", "model").index()
					.del("team", "ulpgc")
					.terminate();
			assertThat(index.open("11","o").terms()).containsExactly(new Term("name","jose"));
			assertThat(index.open("123456", "model").terms()).containsExactly(terms("t=simulation\nuser=mcaballero@gmail.com\nuser=josejuan@gmail.com\nproject=ulpgc"));
		}
	}

	@Test
	public void should_search_using_contain_and_fit_filters() throws Exception {
		File file = File.createTempFile("file", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("123456", "model").index()
					.put("description", "simulation")
					.put("user", "josejuan@gmail.com")
					.put("project", "ulpgc")
					.put("description", "simulation")
					.put("team", "ulpgc.es")
					.put("access", ".*@ulpgc\\.es")
					.terminate();
			index.create("654321","model").index()
					.put("description", "simulation")
					.put("user", "mcaballero@gmail.com")
					.put("project", "ulpgc")
					.terminate();

			index.create("123456", "model").index()
					.del("team", "ulpgc")
					.terminate();
			assertThat(index.subjects().where("team", "project").contains("ulpgc es")).containsExactly(subjects("123456.model"));
			assertThat(index.subjects().where("description").contains("sim")).containsExactly(subjects("123456.model\n654321.model"));
			assertThat(index.subjects().where("description").contains("xxx")).containsExactly(subjects(""));
			assertThat(index.subjects().where("team").contains("ulpgc")).containsExactly(new Subject("123456.model"));
			assertThat(index.subjects().where("access").accepts("jose@gmail.com")).isEmpty();
			assertThat(index.subjects().where("access").accepts("jose@ulpgc.es")).containsExactly(subjects("123456.model"));
			assertThat(index.subjects().where("description", "user").contains("gmail")).containsExactly(subjects("123456.model\n654321.model"));
		}
	}

	@Test
	public void should_index_query_with_terms_and_support_deletion() throws Exception {
		File file = File.createTempFile("items", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("P001.model")
					.index()
					.put("name", "AI Research")
					.put("lead", "alice@example.com")
					.put("status", "active")
					.terminate();

			index.create("P001.model/E1.experiment")
					.index()
					.put("name", "Language Model Evaluation")
					.put("dataset", "OpenQA")
					.terminate();

			index.create("P001.model/E2.experiment").index()
					.put("name", "Graph Alignment")
					.put("dataset", "wikipedia")
					.put("status", "archived")
					.terminate();

			index.create("P001.model/E2.experiment").index()
					.del("status", "archived")
					.terminate();

			index.open("P001.model").children().first().rename("E001");
			index.open("P001.model").children().collect().get(1).rename("E002");
			index.open("P001.model/E001.experiment").drop();

			assertThat(index.open("P001.model").isNull()).isFalse();
			assertThat(index.open("P001.model/E001.experiment")).isNull();

			assertThat(index.open("P001.model/E002.experiment").terms()).doesNotContain(terms("status=archived"));

			assertThat(index.open("P001.model").children().collect()).hasSize(1);
			assertThat(index.open("P001.model").children().first().name()).isEqualTo("E002");

			assertThat(index.subjects().type("model").with("name", "AI Research").collect()).contains(subject("P001.model"));
		}
	}

	@Test
	public void should_dump_index() throws Exception {
		File file = File.createTempFile("items", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file)).restore(inputStream())){
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			index.dump(os);
		}
	}

	@Test
	public void should_support_defragmentation() throws Exception {
		File file = File.createTempFile("items", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file)).restore(inputStream())) {
			index.open("P001.model").index()
					.set("status", "running")
					.set("lead", "alice@example.com")
					.terminate();
			index.open("P002.model").index()
					.del("lead")
					.terminate();
			index.open("P003.model").index()
					.del("lead")
					.del("status")
					.terminate();
			index.open("P004.model").index()
					.del("lead")
					.del("status")
					.terminate();
			index.open("P004.model").index()
					.del("lead")
					.del("status")
					.terminate();
			index.open("P003.model").index()
					.set("lead","mary@example.com")
					.set("status", "running")
					.terminate();

			assertThat(index.subjects().collect().size()).isEqualTo(125);
			assertThat(index.terms().size()).isEqualTo(53);
			assertThat(index.open("P001.model").terms()).contains(new Term("status","running"));
			assertThat(index.open("P001.model").terms()).doesNotContain(new Term("lead","user1@example.com"));
			assertThat(index.open("P001.model").terms()).contains(new Term("lead","alice@example.com"));
			assertThat(index.open("P002.model").terms()).contains(new Term("status","active"));
			assertThat(index.open("P003.model").terms()).containsExactly(new Term("status","running"), new Term("lead","mary@example.com"), new Term("name","Project 3"));
			assertThat(index.open("P004.model").terms()).containsExactly(new Term("name","Project 4"));
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))) {
			assertThat(index.terms().size()).isEqualTo(53);
			assertThat(index.subjects().collect().size()).isEqualTo(125);
			assertThat(index.open("P001.model").terms()).contains(new Term("status","running"));
			assertThat(index.open("P001.model").terms()).doesNotContain(new Term("lead","user1@example.com"));
			assertThat(index.open("P001.model").terms()).contains(new Term("lead","alice@example.com"));
			assertThat(index.open("P002.model").terms()).contains(new Term("status","active"));
			assertThat(index.open("P003.model").terms()).containsExactly(new Term("status","running"), new Term("lead","mary@example.com"), new Term("name","Project 3"));
			assertThat(index.open("P004.model").terms()).containsExactly(new Term("name","Project 4"));
		}
	}

	@Test
	public void name() throws Exception {
		File file = File.createTempFile("items", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file)).restore(inputStream())) {
			index.subjects().with("","").where("dsl").matches(v->v.startsWith(""));
		}
	}

	private static InputStream inputStream() {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream("subjects.txt");
	}

	//TODO test de drop
	//TODO test de rename de child
}
