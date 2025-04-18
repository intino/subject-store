package tests.index;


import org.junit.Test;
import systems.intino.datamarts.subjectstore.model.Term;
import tests.Storages;
import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;

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
		assertThat(index.subjects().roots().serialize()).isEqualTo("11.o");
		assertThat(index.subjects("o").roots().serialize()).isEqualTo("11.o");
		assertThat(index.subjects("p").roots().serialize()).isEqualTo("");
		assertThat(index.subjects().with("name", "jose").roots().serialize()).isEqualTo("11.o");
		assertThat(index.subjects().without("name", "jose").roots().serialize()).isEqualTo("");
		assertThat(index.subjects().without("name", "mario").roots().serialize()).isEqualTo("11.o");
		assertThat(index.subjects().without("name", "mario").roots().filter(a -> a.is("o")).serialize()).isEqualTo("11.o");
		assertThat(index.subjects().without("name", "mario").roots().filter(a -> a.is("user")).serialize()).isEqualTo("");
	}

	@Test
	public void should_support_rename_subjects() throws Exception {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("11", "o").index().put("name", "jose").terminate();
			index.get("11", "o").rename("22");
			assertThat(index.get("11", "o")).isNull();
			assertThat(index.get("22", "o").terms().serialize()).isEqualTo("name=jose");
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			assertThat(index.get("11","o")).isNull();
			assertThat(index.get("22", "o").terms().serialize()).isEqualTo("name=jose");
		}
	}

	@Test
	public void should_support_set_and_del_terms() throws Exception {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			index.create("11", "o").index().set("name", "jose").terminate();
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("name=jose");
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.get("11", "o");
			assertThat(subject.terms().serialize()).isEqualTo("name=jose");
			subject.index().set("name", "mario").terminate();
			assertThat(subject.terms().serialize()).isEqualTo("name=mario");
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.get("11", "o");
			assertThat(subject.terms().serialize()).isEqualTo("name=mario");
			subject.index().del("name").terminate();
			assertThat(subject.terms().serialize()).isEqualTo("");
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.get("11", "o");
			assertThat(subject.terms().serialize()).isEqualTo("");
			subject.index().put("name", "mario").terminate();
			subject.index().put("name", "jose").terminate();
			assertThat(subject.terms().serialize()).isEqualTo("name=mario\nname=jose");
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.get("11", "o");
			assertThat(subject.terms().serialize()).isEqualTo("name=mario\nname=jose");
			subject.index().del("name").terminate();
			assertThat(subject.terms().serialize()).isEqualTo("");
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			Subject subject = index.get("11", "o");
			assertThat(subject.terms().serialize()).isEqualTo("");
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

			assertThat(index.subjects().roots().size()).isEqualTo(1);
			assertThat(index.subjects().all().size()).isEqualTo(3);
			assertThat(index.subjects("o").all().size()).isEqualTo(1);
			assertThat(index.subjects("p").all().size()).isEqualTo(1);
			assertThat(index.subjects("p").all().get(0).identifier()).isEqualTo("1.o/12.p");
			assertThat(index.subjects().roots().get(0)).isEqualTo(Subject.of("1.o"));
			assertThat(index.subjects().roots().get(0).children().get(0)).isEqualTo(Subject.of("1.o/12.p"));
			assertThat(index.get("1","o").isNull()).isFalse();
			assertThat(index.get("1","o").parent().isNull()).isTrue();
			assertThat(index.get("2" ,"o")).isNull();
			assertThat(index.get("2" ,"p")).isNull();
			assertThat(index.get("1.o/12.p").parent()).isEqualTo(index.get("1","o"));
			assertThat(index.get("1.o/12.p").children()).containsExactly(index.get("1.o/12.p/123.q"));
			assertThat(index.get("1.o/12.p").children().get(0).identifier()).isEqualTo("1.o/12.p/123.q");
			assertThat(index.get("1.o/12.p/123.q").parent().parent()).isEqualTo(Subject.of("1.o"));
			assertThat(index.subjects().with("value", "1").roots().size()).isEqualTo(1);
			assertThat(index.subjects("o").with("value", "1").roots().size()).isEqualTo(1);
			assertThat(index.subjects("p").with("value", "1").roots().size()).isEqualTo(0);
			assertThat(index.subjects("p").with("value", "2").all().size()).isEqualTo(1);
			assertThat(index.subjects("p").with("value", "2").all().get(0)).isEqualTo(Subject.of("1.o/12.p"));
			assertThat(index.subjects().with("value", "2").roots().isEmpty()).isTrue();
		}
	}

	@Test
	public void should_support_drop_subjects() throws Exception {
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
			index.get("11.o").drop();
			assertThat(index.subjects().roots().serialize()).isEqualTo("22.o");
			assertThat(index.terms().serialize()).isEqualTo("team=first\nname=luis");
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))){
			assertThat(index.subjects().roots().serialize()).isEqualTo("22.o");
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
			assertThat(index.terms().serialize()).isEqualTo("description=simulation\nuser=mcaballero@gmail.com\nt=simulation\nuser=josejuan@gmail.com");
			index.create("654321","model").index()
					.del("t","simulation")
					.terminate();
			assertThat(index.terms().serialize()).isEqualTo("description=simulation\nuser=mcaballero@gmail.com\nuser=josejuan@gmail.com");
			assertThat(index.subjects().roots().serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects().roots().serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects().with("user","mcaballero@gmail.com").roots().serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects().with("description","simulation").roots().serialize()).isEqualTo("123456.model");
			assertThat(index.subjects().with("user", "josejuan@gmail.com").roots().serialize()).isEqualTo("654321.model");
			assertThat(index.subjects().without("description","simulation").roots().serialize()).isEqualTo("654321.model");
			assertThat(index.subjects().without("user", "josejuan@gmail.com").roots().serialize()).isEqualTo("123456.model");
		}
	}

	@Test
	public void should_get_terms_of_subject() throws Exception {
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
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("name=jose");
			assertThat(index.get("123456", "model").terms().serialize()).isEqualTo("t=simulation\nuser=mcaballero@gmail.com\nuser=josejuan@gmail.com\nproject=ulpgc");
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
			assertThat(index.subjects().where("team", "project").contains("ulpgc es").serialize()).isEqualTo("123456.model");
			assertThat(index.subjects().where("description", "user").contains("gmail").serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects().where("description").contains("sim").serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects().where("description").contains("xxx").serialize()).isEqualTo("");
			assertThat(index.subjects().where("team").contains("ulpgc").serialize()).isEqualTo("123456.model");
			assertThat(index.subjects().where("access").accepts("jose@gmail.com").serialize()).isEqualTo("");
			assertThat(index.subjects().where("access").accepts("jose@ulpgc.es").serialize()).isEqualTo("123456.model");
		}
	}

	@Test
	public void should_index_subjects_with_terms_and_support_deletion() throws Exception {
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

			index.get("P001.model").children().get(0).rename("E001");
			index.get("P001.model").children().get(1).rename("E002");
			index.get("P001.model/E001.experiment").drop();

			assertThat(index.get("P001.model").isNull()).isFalse();
			assertThat(index.get("P001.model/E001.experiment")).isNull();

			assertThat(index.get("P001.model/E002.experiment").terms()
					.serialize()).doesNotContain("status=archived");

			assertThat(index.get("P001.model").children()).hasSize(1);
			assertThat(index.get("P001.model").children().get(0).name()).isEqualTo("E002");

			assertThat(index.subjects("model").with("name", "AI Research").all().serialize()).contains("P001.model");
		}
	}

	@Test
	public void should_dump_index() throws Exception {
		File file = File.createTempFile("items", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file)).restore(inputStream())){
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			index.dump(os);
			assertThat(os.toString()).isEqualTo(new String(inputStream().readAllBytes()));
		}
	}

	@Test
	public void should_support_defragmentation() throws Exception {
		File file = File.createTempFile("items", ".inx");
		try (SubjectIndex index = new SubjectIndex(Storages.in(file)).restore(inputStream())) {
			index.get("P001.model").index()
					.set("status", "running")
					.set("lead", "alice@example.com")
					.terminate();
			index.get("P002.model").index()
					.del("lead")
					.terminate();
			index.get("P003.model").index()
					.del("lead")
					.del("status")
					.terminate();
			index.get("P004.model").index()
					.del("lead")
					.del("status")
					.terminate();
			index.get("P004.model").index()
					.del("lead")
					.del("status")
					.terminate();
			index.get("P003.model").index()
					.set("lead","mary@example.com")
					.set("status", "running")
					.terminate();

			assertThat(index.subjects().all().size()).isEqualTo(125);
			assertThat(index.terms().size()).isEqualTo(53);
			assertThat(index.get("P001.model").terms()).contains(new Term("status","running"));
			assertThat(index.get("P001.model").terms()).doesNotContain(new Term("lead","user1@example.com"));
			assertThat(index.get("P001.model").terms()).contains(new Term("lead","alice@example.com"));
			assertThat(index.get("P002.model").terms()).contains(new Term("status","active"));
			assertThat(index.get("P003.model").terms()).containsExactly(new Term("status","running"), new Term("lead","mary@example.com"), new Term("name","Project 3"));
			assertThat(index.get("P004.model").terms()).containsExactly(new Term("name","Project 4"));
		}
		try (SubjectIndex index = new SubjectIndex(Storages.in(file))) {
			assertThat(index.terms().size()).isEqualTo(53);
			assertThat(index.subjects().all().size()).isEqualTo(125);
			assertThat(index.get("P001.model").terms()).contains(new Term("status","running"));
			assertThat(index.get("P001.model").terms()).doesNotContain(new Term("lead","user1@example.com"));
			assertThat(index.get("P001.model").terms()).contains(new Term("lead","alice@example.com"));
			assertThat(index.get("P002.model").terms()).contains(new Term("status","active"));
			assertThat(index.get("P003.model").terms()).containsExactly(new Term("status","running"), new Term("lead","mary@example.com"), new Term("name","Project 3"));
			assertThat(index.get("P004.model").terms()).containsExactly(new Term("name","Project 4"));
		}
	}

	private static InputStream inputStream() {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream("subjects.txt");
	}

	//TODO test de drop
	//TODO test de rename de child
}
