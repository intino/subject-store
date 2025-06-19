package tests;

import org.junit.Test;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

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
		assertThat(subject.isChildOf(subject.parent())).isTrue();
		assertThat(subject.isUnderOf(subject.parent())).isTrue();
	}

	@Test
	public void should_create_children_without_context() {
		Subject subject = Subject.of("a", "model");
		Subject child = subject.create("b", "release");
		Subject grandson = child.create("c", "properties");

		assertThat(child.typedName()).isEqualTo("b.release");
		assertThat(child.parent()).isEqualTo(subject);
		assertThat(grandson.parent().parent()).isEqualTo(subject);
		assertThat(grandson.isUnderOf(subject)).isTrue();
		assertThat(grandson.isUnderOf(child)).isTrue();
		assertThat(child.isUnderOf(subject)).isTrue();
		assertThat(subject.isUnderOf(child)).isFalse();
		assertThat(subject.children().collect()).containsExactly(Subject.of("a.model/b.release"));
		assertThat(subject.children().first().children().first()).isEqualTo(Subject.of("a.model/b.release/c.properties"));
	}

	@Test
	public void should_write_isChildOf_system_error_when_subject_context_is_not_defined() {
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

	@Test
	public void name() {
		Subject subject = Subject.of("ulpgc.flogo", "collection");
		assertThat(subject.name()).isEqualTo("ulpgc.flogo");
		assertThat(subject.type()).isEqualTo("");
		assertThat(subject.identifier()).isEqualTo("ulpgc.flogo.collection");
	}
}
