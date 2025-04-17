package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Subjects;
import systems.intino.datamarts.subjectstore.model.Term;

public interface SubjectQuery {
	Subjects all();

	Subjects roots();

	SubjectFilter with(String tag, String value);

	SubjectFilter without(String tag, String value);

	AttributeFilter where(String... keys);

	interface AttributeFilter {
		Subjects contains(String value);

		Subjects matches(String value);
	}

	interface SubjectFilter {
		Subjects all();

		Subjects roots();

		SubjectFilter with(Term term);

		SubjectFilter without(Term term);

		default SubjectFilter with(String tag, String value) {
			return with(new Term(tag, value));
		}

		default SubjectFilter without(String tag, String value) {
			return without(new Term(tag, value));
		}

	}
}
