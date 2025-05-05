package tests.index;


import org.junit.Test;
import systems.intino.datamarts.subjectstore.view.index.Column.Type;
import tests.Storages;
import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.SubjectIndexView;
import systems.intino.datamarts.subjectstore.io.Statements;
import systems.intino.datamarts.subjectstore.io.statements.TabularStatements;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.datamarts.subjectstore.SubjectIndexView.*;
import static systems.intino.datamarts.subjectstore.view.index.Column.Type.Text;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndexView_ {

	@Test
	public void should_create_views_including_summary() throws Exception {
		try (SubjectIndex index = new SubjectIndex(Storages.inMemory()).restore(inputStream("subjects.txt"))) {
			SubjectIndexView models = SubjectIndexView.of(index)
					.type("model")
					.add("status", Type.Text)
					.add("name", Type.Text)
					.build();
			assertThat(models.size()).isEqualTo(25);
			//TODO assertThat(models.column("status").stats().categories()).containsExactly("active");
			//TODO assertThat(models.column("name").stats().categories().size()).isEqualTo(25);
			//TODO assertThat(experiments.size()).isEqualTo(100);
			//TODO assertThat(experiments.column("status").stats().categories()).containsExactly("running", "queued", "error", "done");
			//TODO assertThat(experiments.column("status").stats().frequency("running")).isEqualTo(25);}
		}
	}

	@Test
	public void should_calculate_summary_frequencies_consistently() throws Exception {
		try (SubjectIndex index = new SubjectIndex(Storages.inMemory()).consume(statements())) {
			assertThat(index.subjects().isRoot().size()).isEqualTo(435);
			SubjectIndexView view = SubjectIndexView.of(index)
					.type("port")
					.add("name", Text)
					.add("country", Text)
					.add("cabotage-region", Type.Text)
					.add("draft", Type.Number)
					.add("cost-per-full", Type.Number)
					.add("cost-per-full-transfer", Type.Number)
					.sort("draft", SortDirection.Ascending)
					.sort("name", SortDirection.Ascending)
					.build();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			view.exportTo(os);
			assertThat(os.toString().trim()).isEqualTo(content());
		}
	}

	private static String content() throws IOException {
		try (InputStream is = inputStream("ports-filtered-sorted.tsv")) {
			return new String(is.readAllBytes()).trim();
		}
	}

	private Statements statements() {
		InputStream is = inputStream("ports.tsv");
		Statements statements = new TabularStatements(is, "\t");
		statements.schema()
				.map("id", s-> s + ".port")
				.map("latitude", s-> null)
				.map("longitude", s-> null)
				.map("draft", s->s)
				.map("cost-per-full", s->s)
				.map("cost-per-full-transfer", s->s)
				.map("cost-port-call-fixed", s->s)
				.map("cost-port-call-per-ffe", s->s);
		return statements;
	}

	private static InputStream inputStream(String name) {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream(name);
	}
}
