package tests.index;


import org.junit.Test;
import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.SubjectIndexView;
import systems.intino.datamarts.subjectstore.index.io.Statements;
import systems.intino.datamarts.subjectstore.index.io.statements.TabularStatements;
import systems.intino.datamarts.subjectstore.index.model.Subject;
import systems.intino.datamarts.subjectstore.index.model.Subjects;
import systems.intino.datamarts.subjectstore.index.view.Column;
import systems.intino.datamarts.subjectstore.index.view.Summary;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndexView_ {

	@Test
	public void should_create_views_including_summary() throws IOException {
		try (SubjectIndex index = new SubjectIndex().restore(inputStream("subjects.txt"))) {
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
			assertThat(models.column("status").summary().categories()).containsExactly("active");
			assertThat(models.column("name").summary().categories().size()).isEqualTo(25);
			assertThat(experiments.size()).isEqualTo(100);
			assertThat(experiments.column("status").summary().categories()).containsExactly("running", "queued", "error", "done");
			assertThat(experiments.column("status").summary().frequency("running")).isEqualTo(25);
		}
	}

	@Test
	public void should_calculate_summary_frequencies_consistently() throws IOException {
		try (SubjectIndex index = new SubjectIndex().consume(statements())) {
			assertThat(index.subjects().roots().size()).isEqualTo(435);
			SubjectIndexView view = SubjectIndexView.of(index)
					.type("port")
					.add("country")
					.add("cabotage-region")
					.add("draft")
					.add("cost-per-full")
					.add("cost-per-full-transfer")
					.build();
			for (Column column : view) {
				Summary summary = column.summary();
				SubjectIndex.SubjectQuery query = index.subjects("port");
				List<Subject> all = new ArrayList<>(query.all().items());
				for (String category : summary.categories()) {
					Subjects x = query.with(column.name(), category).all();
					all.removeAll(x.items());
					assertThat(summary.frequency(category)).isEqualTo(x.size());
				}
				assertThat(all.size()).isNotEqualTo(0);
			}
		}
	}

	private Statements statements() {
		InputStream is = inputStream("ports.tsv");
		Statements feeder = new TabularStatements(is, "\t");
		feeder.schema()
				.map("id", s-> s + ".port")
				.map("latitude", s-> null)
				.map("longitude", s-> null)
				.map("id", s-> s + ".port")
				.map("draft", this::range2)
				.map("cost-per-full", this::range100)
				.map("cost-per-full-transfer", this::range100)
				.map("cost-port-call-fixed", this::range100)
				.map("cost-port-call-per-ffe", this::range100);
		return feeder;
	}

	private String range2(String value) {
		return range(value, 2);
	}

	private String range100(String value) {
		return range(value, 100);
	}

	private static String range(String value, int bin) {
		int v = (int) (Double.parseDouble(value) / bin);
		return '[' + String.valueOf(v * bin) + "-" + (v + 1) * bin + ')';
	}


	private static InputStream inputStream(String name) {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream(name);
	}
}
