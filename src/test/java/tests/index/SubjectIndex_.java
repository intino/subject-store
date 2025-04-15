package tests.index;


import org.junit.Test;
import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.index.model.Subject;
import systems.intino.datamarts.subjectstore.index.model.Subjects;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndex_ {

	@Test
	public void should_support_index_subject_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.create("11", "o").update().put("name", "jose").commit();
			check(index);
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.create("11", "o").update()
					.put("name", "jose")
					.commit();
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
	public void should_support_rename_subjects() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.create("11", "o").update().put("name", "jose").commit();
			index.create("11", "o").update().rename("22").commit();
			checkRename(index);
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			checkRename(index);
		}
	}

	private static void checkRename(SubjectIndex index)  {
		assertThat(index.get("11","o")).isNull();
		assertThat(index.get("22", "o").terms().serialize()).isEqualTo("name=jose");
	}

	@Test
	public void should_support_set_and_del_terms() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.create("11", "o").update().set("name", "jose").commit();
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("name=jose");
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("name=jose");
			index.create("11", "o").update().set("name", "mario").commit();
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("name=mario");
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("name=mario");
			index.create("11", "o").update().del("name").commit();
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("");
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("");
			index.create("11", "o").update().put("name", "mario").commit();
			index.create("11", "o").update().put("name", "jose").commit();
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("name=jose\nname=mario");
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("name=jose\nname=mario");
			index.create("11", "o").update().del("name").commit();
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("");
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("");
		}
	}

	@Test
	public void should_navigate_subject_structure() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			Subject s1 = index.create("11","o");
			s1.update().put("value", "1").commit();
			Subject s2 = s1.create("22", "p");
			s2.update().put("value", "2").commit();
			Subject s3 = s2.create("33", "q");
			s3.update().put("value", "3").commit();
			Subject s4 = s2.create("44", "q");
			s4.update().put("value", "4").commit();
			s4.drop();

			assertThat(index.subjects().roots().size()).isEqualTo(1);
			assertThat(index.subjects().all().size()).isEqualTo(3);
			assertThat(index.subjects("o").all().size()).isEqualTo(1);
			assertThat(index.subjects("p").all().size()).isEqualTo(1);
			assertThat(index.subjects("p").all().get(0).identifier()).isEqualTo("11.o/22.p");
			assertThat(index.subjects().roots().get(0)).isEqualTo(Subject.of("11.o"));
			assertThat(index.subjects().roots().get(0).children().get(0)).isEqualTo(Subject.of("11.o/22.p"));
			assertThat(index.get("11","o").isNull()).isFalse();
			assertThat(index.get("11","o").parent().isNull()).isTrue();
			assertThat(index.get("12" ,"o")).isNull();
			assertThat(index.get("11.o/22.p").parent()).isEqualTo(index.get("11","o"));
			assertThat(index.get("11.o/22.p").children()).containsExactly(index.get("11.o/22.p/33.q"));
			assertThat(index.get("11.o/22.p").children().get(0).identifier()).isEqualTo("11.o/22.p/33.q");
			assertThat(index.get("11.o/22.p/33.q").parent().parent()).isEqualTo(Subject.of("11.o"));
			assertThat(index.subjects().with("value", "1").roots().size()).isEqualTo(1);
			assertThat(index.subjects("o").with("value", "1").roots().size()).isEqualTo(1);
			assertThat(index.subjects("p").with("value", "1").roots().size()).isEqualTo(0);
			assertThat(index.subjects("p").with("value", "2").all().size()).isEqualTo(1);
			assertThat(index.subjects("p").with("value", "2").all().get(0)).isEqualTo(Subject.of("11.o/22.p"));
			assertThat(index.subjects().with("value", "2").roots().isEmpty()).isTrue();
		}
	}

	@Test
	public void should_support_drop_subjects() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			Subject subject = index.create("11", "o");
			subject.update()
					.put("name", "jose")
					.put("team", "first")
					.commit();
			index.create("22","o").update()
					.put("name", "luis")
					.put("team", "first")
					.commit();
			index.get("11.o").drop();
			assertThat(index.subjects().roots().serialize()).isEqualTo("22.o");
			assertThat(index.terms().serialize()).isEqualTo("team=first\nname=luis");
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			assertThat(index.subjects().roots().serialize()).isEqualTo("22.o");
		}
	}

	@Test
	public void should_support_unset_terms_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.create("123456", "model").update()
					.put("description","simulation")
					.put("user", "mcaballero@gmail.com")
					.commit();
			index.create("654321","model").update()
					.put("t","simulation")
					.put("user", "mcaballero@gmail.com")
					.put("user", "josejuan@gmail.com")
					.commit();
			index.create("654321","model").update()
					.del("t","simulation")
					.commit();
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
	public void should_get_terms_of_subject() throws IOException {
		File file = File.createTempFile("file", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.create("11", "o").update()
					.put("name", "jose")
					.commit();
			index.create("123456", "model").update()
					.put("t", "simulation")
					.put("user", "mcaballero@gmail.com")
					.put("user", "josejuan@gmail.com")
					.put("project", "ulpgc")
					.put("t", "simulation")
					.put("team", "ulpgc")
					.commit();
			index.create("123456", "model").update()
					.del("team", "ulpgc")
					.commit();
			assertThat(index.get("11","o").terms().serialize()).isEqualTo("name=jose");
			assertThat(index.get("123456", "model").terms().serialize()).isEqualTo("t=simulation\nuser=mcaballero@gmail.com\nuser=josejuan@gmail.com\nproject=ulpgc");
		}
	}

	@Test
	public void should_search_using_contain_and_fit_filters() throws IOException {
		File file = File.createTempFile("file", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.create("123456", "model").update()
					.put("description", "simulation")
					.put("user", "josejuan@gmail.com")
					.put("project", "ulpgc")
					.put("description", "simulation")
					.put("team", "ulpgc.es")
					.put("access", ".*@ulpgc\\.es")
					.commit();
			index.create("654321","model").update()
					.put("description", "simulation")
					.put("user", "mcaballero@gmail.com")
					.put("project", "ulpgc")
					.commit();

			index.create("123456", "model").update()
					.del("team", "ulpgc")
					.commit();
			assertThat(index.subjects().where("team", "project").contains("ulpgc es").serialize()).isEqualTo("123456.model");
			assertThat(index.subjects().where("description", "user").contains("gmail").serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects().where("description").contains("sim").serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects().where("description").contains("xxx").serialize()).isEqualTo("");
			assertThat(index.subjects().where("team").contains("ulpgc").serialize()).isEqualTo("123456.model");
			assertThat(index.subjects().where("access").matches("jose@gmail.com").serialize()).isEqualTo("");
			assertThat(index.subjects().where("access").matches("jose@ulpgc.es").serialize()).isEqualTo("123456.model");
		}
	}

	@Test
	public void should_index_subjects_with_terms_and_support_deletion() throws IOException {
		File file = File.createTempFile("items", ".inx");

		try (SubjectIndex index = new SubjectIndex(file)) {
			index.create("P001.model").update()
					.put("name", "AI Research")
					.put("lead", "alice@example.com")
					.put("status", "active")
					.commit();

			index.create("P001.model/E001.experiment").update()
					.put("name", "Language Model Evaluation")
					.put("dataset", "OpenQA")
					.commit();

			index.create("P001.model/E002.experiment").update()
					.put("name", "Graph Alignment")
					.put("dataset", "DBpedia")
					.put("status", "archived")
					.commit();

			index.create("P001.model/E002.experiment").update()
					.del("status", "archived")
					.commit();

			index.get("P001.model/E001.experiment").drop();


			index.create("P001.model/E002.experiment").update();

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
	public void should_dump_index() throws IOException {
		File file = File.createTempFile("items", ".inx");
		try (SubjectIndex index = new SubjectIndex(file).restore(inputStream())) {
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			index.dump(os);
			assertThat(os.toString()).isEqualTo(new String(inputStream().readAllBytes()));
		}
	}

	@Test
	public void should_support_defragmentation() throws Exception {
		File file = File.createTempFile("items", ".inx");
		try (SubjectIndex index = new SubjectIndex(file).restore(inputStream())) {
			assertThat(index.subjects().roots().size()).isEqualTo(25);
			Subjects subjects = index.subjects().all();
			assertThat(subjects.size()).isEqualTo(125);
			assertThat(index.terms().size()).isEqualTo(55);
			int nonRootDeleted = 0;
			int rootDeleted = 0;
			for (int i = 0; i < 125; i += 3) {
				Subject subject = subjects.get(i);
				subject.drop();
				if (subject.isRoot()) rootDeleted++; else nonRootDeleted++;
			}
			assertThat(nonRootDeleted).isEqualTo(33);
			assertThat(rootDeleted).isEqualTo(9);
			assertThat(index.subjects().all().size()).isEqualTo(56);
			assertThat(index.subjects().roots().size()).isEqualTo(16);
			assertThat(index.terms().size()).isEqualTo(37);
			assertThat(index.isFragmented()).isTrue();

			File file2 = File.createTempFile("items", ".inx");
			try (SubjectIndex index2 = new SubjectIndex(file2)) {
				index.copyTo(index2);
				assertThat(index2.subjects().all().size()).isEqualTo(56);
				assertThat(index2.subjects().roots().size()).isEqualTo(16);
				assertThat(index2.terms().size()).isEqualTo(37);
				assertThat(index2.isFragmented()).isFalse();
			}
			try (SubjectIndex index2 = new SubjectIndex(file2)) {
				assertThat(index2.subjects().all().size()).isEqualTo(56);
				assertThat(index2.subjects().roots().size()).isEqualTo(16);
				assertThat(index2.terms().size()).isEqualTo(37);
				assertThat(index2.isFragmented()).isFalse();
			}

		}
	}

	private static InputStream inputStream() {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream("subjects.txt");
	}
}
