package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Subject;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface SubjectQuery {
	SubjectQuery Empty = emptyQuery();

	int size();

	boolean isEmpty();

	Subject first();

	Stream<Subject> stream();

	List<Subject> collect();

	SubjectQuery type(String... types);

	SubjectQuery isRoot();

	SubjectQuery with(String tag, String value);

	SubjectQuery without(String tag, String value);

	AttributeFilter where(String... tags);

	interface AttributeFilter {

		AttributeFilter Empty = emptyFilter();

		List<Subject> contains(String value);
		List<Subject> accepts(String value);
		List<Subject> matches(Predicate<String> predicate);
	}

	static SubjectQuery emptyQuery() {
		return new SubjectQuery() {
			@Override
			public int size() {
				return 0;
			}

			@Override
			public boolean isEmpty() {
				return true;
			}

			@Override
			public Subject first() {
				return Subject.of("");
			}

			@Override
			public Stream<Subject> stream() {
				return Stream.empty();
			}

			@Override
			public List<Subject> collect() {
				return List.of();
			}

			@Override
			public SubjectQuery type(String... types) {
				return this;
			}

			@Override
			public SubjectQuery isRoot() {
				return this;
			}

			@Override
			public SubjectQuery with(String tag, String value) {
				return this;
			}

			@Override
			public SubjectQuery without(String tag, String value) {
				return this;
			}

			@Override
			public AttributeFilter where(String... tags) {
				return AttributeFilter.Empty;
			}
		};
	}

	static AttributeFilter emptyFilter() {
		return new AttributeFilter() {
			@Override
			public List<Subject> contains(String value) {
				return List.of();
			}

			@Override
			public List<Subject> accepts(String value) {
				return List.of();
			}

			@Override
			public List<Subject> matches(Predicate<String> predicate) {
				return List.of();
			}
		};
	}
}
