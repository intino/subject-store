package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.helpers.PatternFactory;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.Double.parseDouble;

public interface SubjectQuery {
	SubjectQuery Empty = emptyQuery();

	int size();
	boolean isEmpty();
	Subject first();
	default Subject last() {
		List<Subject> list = collect();
		return list.isEmpty() ? null : list.getLast();
	}
	Stream<Subject> stream();
	List<Subject> collect();

	SubjectQuery nameStartsWith(String value);
	SubjectQuery nameContains(String value);
	SubjectQuery nameEndsWith(String value);

	SubjectQuery isType(String type);
	SubjectQuery isRoot();

	default SubjectQuery isChildOf(Subject subject) {
		return isChildOf(subject.identifier());
	}
	SubjectQuery isChildOf(String identifier);

	default SubjectQuery isUnderOf(Subject subject) {
		return isUnderOf(subject.identifier());
	}
	SubjectQuery isUnderOf(String identifier);

	default SubjectQuery orderBy(String tag) {
		return orderBy(tag, String::compareTo);
	}
	default SubjectQuery orderBy(String tag, OrderType type) {
		return orderBy(tag, type::compare);
	}
	SubjectQuery orderBy(String tag, Comparator<String> comparator);

	AttributeFilter where(String tag);


	interface AttributeFilter {
		default SubjectQuery equals(String value) {
			return satisfy(v -> v.equalsIgnoreCase(value));
		}
		default SubjectQuery contains(String... values) {
			return satisfy(v -> Arrays.stream(values).allMatch(v::contains));
		}

		default SubjectQuery accepts(String value) {
			return satisfy(v-> PatternFactory.pattern(v).matcher(value).matches());
		}

		SubjectQuery satisfy(Predicate<String> predicate);

	}

	static SubjectQuery emptyQuery() {
		return new SubjectQuery() {
			private final SubjectQuery This = this;
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
			public SubjectQuery isType(String type) {
				return this;
			}

			@Override
			public SubjectQuery isRoot() {
				return this;
			}

			@Override
			public SubjectQuery isChildOf(String identifier) {
				return this;
			}

			@Override
			public SubjectQuery isUnderOf(String identifier) {
				return this;
			}

			@Override
			public SubjectQuery orderBy(String tag, Comparator<String> comparator) {
				return this;
			}

			@Override
			public AttributeFilter where(String tag) {
				return predicate -> This;
			}

			@Override
			public SubjectQuery nameStartsWith(String value) {
				return this;
			}

			@Override
			public SubjectQuery nameContains(String value) {
				return this;
			}

			@Override
			public SubjectQuery nameEndsWith(String value) {
				return this;
			}

		};
	}

	class Sorting {
		private final List<Sort> sorts = new ArrayList<>();
		public void add(String tag, Comparator<String> comparator) {
			sorts.add(new Sort(tag, comparator));
		}

		public int sort(Subject s1, Subject s2) {
			for (Sort sort : sorts) {
				String v1 = s1.get(sort.tag());
				String v2 = s2.get(sort.tag());
				int compare = sort.comparator().compare(v1, v2);
				if (compare != 0) return compare;
			}
			return 0;
		}
		private record Sort(String tag, Comparator<String> comparator) {}

	}

	enum OrderType {
			TextAscending, TextDescending,
			NumericAscending, NumericDescending,
			InstantAscending, InstantDescending;

		public int compare(String s1, String s2) {
			if (s1.isEmpty() && s2.isEmpty()) return 0;
			if (s1.isEmpty()) return 1;
			if (s2.isEmpty()) return -1;
			if (this == TextDescending) return s2.compareTo(s1);
			if (this == NumericDescending) return Double.compare(parseDouble(s2), parseDouble(s1));
			if (this == InstantDescending) return Instant.parse(s2).compareTo(Instant.parse(s1));
			if (this == NumericAscending) return Double.compare(parseDouble(s1), parseDouble(s2));
			if (this == InstantAscending) return Instant.parse(s1).compareTo(Instant.parse(s2));
			return s1.compareTo(s2);
		}
	}
}
