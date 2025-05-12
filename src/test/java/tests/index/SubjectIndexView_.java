package tests.index;


import org.junit.Test;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.view.index.Column.Type;
import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.SubjectIndexView;
import systems.intino.datamarts.subjectstore.io.Triples;
import systems.intino.datamarts.subjectstore.io.triples.TabularTriples;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.datamarts.subjectstore.SubjectQuery.OrderType.NumericAscending;
import static systems.intino.datamarts.subjectstore.SubjectQuery.OrderType.NumericDescending;
import static systems.intino.datamarts.subjectstore.view.index.Column.Type.Text;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndexView_ {

	@Test
	public void should_create_views_including_summary() throws Exception {
		SubjectIndex index = new SubjectIndex(new File("index.triples")).restore(inputStream("subjects.triples"));
		SubjectIndexView models = SubjectIndexView.of(index.subjects().type("model").collect())
				.add("status", Type.Text)
				.add("name", Type.Text)
				.build();
		SubjectIndexView experiments = SubjectIndexView.of(index.subjects().type("experiment").collect())
				.add("status", Type.Text)
				.build();
		assertThat(models.size()).isEqualTo(25);
		assertThat(models.column("status").stats().categories()).containsExactly("active");
		assertThat(models.column("name").stats().categories().size()).isEqualTo(25);
		assertThat(experiments.size()).isEqualTo(100);
		assertThat(experiments.column("status").stats().categories()).containsExactly("running", "queued", "error", "done");
		assertThat(experiments.column("status").stats().frequency("running")).isEqualTo(25);
	}

	@Test
	public void should_calculate_summary_frequencies_consistently() throws Exception {
		SubjectIndex index = new SubjectIndex(new File("index.triples")).restore(triples());
		assertThat(index.subjects().isRoot().size()).isEqualTo(435);
		List<Subject> subjects = index.subjects()
				.type("port")
				.orderBy("draft", NumericAscending)
				.orderBy("cost-per-full", NumericDescending)
				.collect();
		SubjectIndexView view = SubjectIndexView.of(subjects)
				.add("name", Text)
				.add("country", Text)
				.add("cabotage-region", Type.Text)
				.add("draft", Type.Number)
				.add("cost-per-full", Type.Number)
				.add("cost-per-full-transfer", Type.Number)
				.build();
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		view.exportTo(os);
		assertThat(os.toString().trim()).isEqualTo(content());
	}

	private static String content() throws IOException {
		try (InputStream is = inputStream("ports-filtered-sorted.tsv")) {
			return new String(is.readAllBytes()).trim();
		}
	}

	private Triples triples() {
		InputStream is = inputStream("ports.tsv");
		Triples triples = new TabularTriples(is, "\t");
		triples.schema()
				.map("id", s-> s + ".port")
				.map("latitude", s-> null)
				.map("longitude", s-> null)
				.map("draft", s->s)
				.map("cost-per-full", s->s)
				.map("cost-per-full-transfer", s->s)
				.map("cost-port-call-fixed", s->s)
				.map("cost-port-call-per-ffe", s->s);
		return triples;
	}

	private static InputStream inputStream(String name) {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream(name);
	}
}
