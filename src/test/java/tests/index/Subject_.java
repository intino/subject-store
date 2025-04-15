package tests.index;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.index.model.Subject;
import systems.intino.datamarts.subjectstore.index.model.Subjects;
import systems.intino.datamarts.subjectstore.index.model.Term;
import systems.intino.datamarts.subjectstore.index.model.Terms;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class Subject_ {
	@Test
	public void should_navigate_to_parent() {
		Subject subject = Subject.of(" a / b.release  ");
		assertThat(subject.typedName()).isEqualTo("b.release");
		assertThat(subject.name()).isEqualTo("b");
		assertThat(subject.type()).isEqualTo("release");
		assertThat(subject.parent().typedName()).isEqualTo("a");
		assertThat(subject.parent().name()).isEqualTo("a");
		assertThat(subject.parent().type()).isEqualTo("");
		assertThat(subject.parent().parent().isNull()).isTrue();
	}

	@Test
	public void should_create_children_without_context() {
		Subject subject = new Subject("a", "model");
		Subject child = subject.create("b", "release");
		Subject grandson = child.create("c", "properties");

		assertThat(child.typedName()).isEqualTo("b.release");
		assertThat(child.parent()).isEqualTo(subject);
		assertThat(grandson.parent().parent()).isEqualTo(subject);
	}

	@Test
	public void should_create_children_with_context() {
		Subject subject = new Subject(" a.model ", context());
		Subject child = subject.create("b", "release");
		Subject grandson = child.create("c", "properties");

		assertThat(child.typedName()).isEqualTo("b.release");
		assertThat(child.parent()).isEqualTo(subject);
		assertThat(grandson.parent().parent()).isEqualTo(subject);

		assertThat(subject.children()).containsExactly(Subject.of("a.model/b.release"));
		assertThat(subject.children().get(0).children().get(0)).isEqualTo(Subject.of("a.model/b.release/c.properties"));
	}

	@Test
	public void should_write_in_system_error_when_subject_context_is_not_defined() {
		PrintStream originalErr = System.err;
		ByteArrayOutputStream errContent = new ByteArrayOutputStream();
		System.setErr(new PrintStream(errContent));
		try {
			Subject.of("a.model").terms();
			String output = errContent.toString().trim();
			assertThat(output).contains("Context is not defined for 'a.model'");

		} finally {
			System.setErr(originalErr);
		}
	}

	private Subject.Context context() {
		return new Subject.Context() {
			@Override
			public Subjects children(Subject subject) {
				return new Subjects(childrenOf(subject));
			}

			@Override
			public Terms terms(Subject subject) {
				return termsOf(subject);
			}

			@Override
			public Subject.Transaction update(Subject subject) {
				return null;
			}

			@Override
			public Subject create(Subject child) {
				childrenOf(child.parent()).add(child);
				return child;
			}

			@Override
			public void rename(String name) {

			}

			@Override
			public void drop(Subject subject) {

			}

			@Override
			public SubjectHistory history(Subject subject) {
				return null;
			}
		};
	}

	private Terms termsOf(Subject subject) {
		return new Terms(List.of(new Term("email", "data@gmail.com")));
	}

	private final Map<Subject, List<Subject>> map = new HashMap<>();
	private List<Subject> childrenOf(Subject subject) {
		return map.computeIfAbsent(subject, k -> new ArrayList<>());
	}
}
