package tests.index;


import org.junit.Test;
import tests.Storages;
import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.SubjectIndexView;
import systems.intino.datamarts.subjectstore.io.Statements;
import systems.intino.datamarts.subjectstore.io.statements.TabularStatements;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndexView_ {

	@Test
	public void should_create_views_including_summary() throws IOException {
		SubjectIndex index = new SubjectIndex(Storages.inMemory()).restore(inputStream("subjects.txt"));
		SubjectIndexView models = SubjectIndexView.of(index)
				.type("model")
				.add("status")
				.add("name")
				.build();
		SubjectIndexView experiments = SubjectIndexView.of(index)
				.type("experiment")
				.add("status")
				.build();

		assertThat(models.size()).isEqualTo(25);
		assertThat(models.column("status").stats().categories()).containsExactly("active");
		assertThat(models.column("name").stats().categories().size()).isEqualTo(25);
		assertThat(experiments.size()).isEqualTo(100);
		assertThat(experiments.column("status").stats().categories()).containsExactly("running", "queued", "error", "done");
		assertThat(experiments.column("status").stats().frequency("running")).isEqualTo(25);
	}

	@Test
	public void should_calculate_summary_frequencies_consistently() throws IOException {
		SubjectIndex index = new SubjectIndex(Storages.inMemory()).consume(statements());
		assertThat(index.query().roots().size()).isEqualTo(435);
		SubjectIndexView view = SubjectIndexView.of(index)
				.type("port")
				.add("name")
				.add("country")
				.add("cabotage-region")
				.add("draft")
				.add("cost-per-full")
				.add("cost-per-full-transfer")
				.build();
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		view.exportTo(os);
		assertThat(os.toString().trim()).isEqualTo(new String(inputStream("ports-filtered.tsv").readAllBytes()).trim());
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
