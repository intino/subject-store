package tests;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.model.Term;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class Subject_ {
	@Test
	public void should_navigate_to_parent() {
		Subject subject = Subject.of(" a / english .b.release  ");
		assertThat(subject.identifier()).isEqualTo("a/english.b.release");
		assertThat(subject.typedName()).isEqualTo("english.b.release");
		assertThat(subject.name()).isEqualTo("english.b");
		assertThat(subject.type()).isEqualTo("release");
		assertThat(subject.parent().typedName()).isEqualTo("a");
		assertThat(subject.parent().name()).isEqualTo("a");
		assertThat(subject.parent().type()).isEqualTo("");
		assertThat(subject.parent().parent().isNull()).isTrue();
		assertThat(subject.parent().open("english.b","release")).isEqualTo(subject);
	}

	@Test
	public void should_create_children_without_context() {
		Subject subject = Subject.of("a", "model", context());
		Subject child = subject.create("b", "release");
		Subject grandson = child.create("c", "properties");

		assertThat(child.typedName()).isEqualTo("b.release");
		assertThat(child.parent()).isEqualTo(subject);
		assertThat(grandson.parent().parent()).isEqualTo(subject);
	}

	@Test
	public void should_create_children_with_context() {
		Subject subject = Subject.of(" a.model ", context());
		Subject child = subject.create("b", "release");
		Subject grandson = child.create("c", "properties");

		assertThat(child.typedName()).isEqualTo("b.release");
		assertThat(child.parent()).isEqualTo(subject);
		assertThat(grandson.parent().parent()).isEqualTo(subject);

		assertThat(subject.children().collect()).containsExactly(Subject.of("a.model/b.release"));
		assertThat(subject.children().first().children().first()).isEqualTo(Subject.of("a.model/b.release/c.properties"));
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
			public List<Subject> children(Subject subject) {
				return childrenOf(subject);
			}

			@Override
			public List<Term> terms(Subject subject) {
				return termsOf(subject);
			}

			@Override
			public Subject.Updating update(Subject subject) {
				return null;
			}

			@Override
			public Subject create(Subject child) {
				childrenOf(child.parent()).add(child);
				return child;
			}

			@Override
			public Subject get(String identifier) {
				return Subject.of(identifier);
			}

			@Override
			public void rename(Subject subject, String name) {
			}

			@Override
			public void drop(Subject subject) {

			}
		};
	}

	private List<Term> termsOf(Subject subject) {
		return List.of(new Term("email", "data@gmail.com"));
	}

	private final Map<Subject, List<Subject>> map = new HashMap<>();
	private List<Subject> childrenOf(Subject subject) {
		return map.computeIfAbsent(subject, k -> new ArrayList<>());
	}
}
