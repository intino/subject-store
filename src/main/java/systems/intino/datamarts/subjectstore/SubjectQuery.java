package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.model.Term;

import java.util.List;
import java.util.function.Predicate;

public interface SubjectQuery {
	int size();

	Subject first();

	List<Subject> collect();

	SubjectQuery type(String... types);

	SubjectFilter roots();

	SubjectFilter with(String tag, String value);

	SubjectFilter without(String tag, String value);

	SubjectFilter that(Predicate<Subject> predicate);

	AttributeFilter where(String... tags);

	interface AttributeFilter {
		List<Subject> contains(String value);
		List<Subject> accepts(String value);
		List<Subject> that(Predicate<String> predicate);
	}

	interface SubjectFilter {
		int size();

		Subject first();

		List<Subject> collect();

		SubjectFilter isRoot();

		SubjectFilter with(Term term);

		SubjectFilter without(Term term);

		SubjectFilter that(Predicate<Subject> predicate);

		default SubjectFilter with(String tag, String value) {
			return with(new Term(tag, value));
		}

		default SubjectFilter without(String tag, String value) {
			return without(new Term(tag, value));
		}

		default boolean isEmpty() {
			return size() == 0;
		}

	}
}
