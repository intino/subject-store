package systems.intino.datamarts.subjectstore.model;

import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectQuery;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectstore.helpers.PatternFactory.pattern;

public final class Subject {
	public static final String Any = "*";
	private final String identifier;
	private final Context context;
	private final List<Term> terms;

	public static Subject of(String identifier) {
		if (identifier == null) return null;
		return new Subject(identifier);
	}

	public Subject(String identifier, Context context) {
		this.identifier = identifier != null ? identifier.trim().replaceAll("\\s+/\\s+", "/") : null;
		this.context = context != null ? context : Context.Null;
		this.terms = new ArrayList<>();
	}

	public Subject(Subject subject, Context context) {
		this(subject.identifier, context);
	}

	public Subject(String identifier) {
		this(identifier, Context.Null);
	}

	public Subject(String name, String type) {
		this(name + "." + type, Context.Null);
	}

	public Subject(Subject parent, String identifier) {
		this(parent.identifier + "/" + identifier, parent.context());
	}

	public Subject(Subject parent, String name, String type) {
		this(parent, name + "." + type);
	}

	public String identifier() {
		return identifier;
	}

	public String typedName() {
		int i = identifier.lastIndexOf('/');
		return identifier.substring(i + 1).trim();
	}

	public String name() {
		String typedName = typedName();
		int i = typedName.lastIndexOf('.');
		return i >= 0 ? typedName.substring(0, i) : typedName;
	}

	public String type() {
		String identifier = typedName();
		int i = identifier.lastIndexOf('.');
		return i >= 0 ? identifier.substring(i + 1) : "";
	}

	public boolean is(String type) {
		return type.equals("*") || type.equals(this.type());
	}

	public boolean isNull() {
		return this.identifier.isEmpty();
	}

	public boolean isRoot() {
		return identifier.lastIndexOf('/') < 0;
	}

	public Subject parent() {
		return new Subject(parentIdentifier(), context);
	}

	public SubjectQuery children() {
		checkIfContextExists();
		return query();
	}

	public String get(String tag) {
		return get(tag, ", ");
	}

	public String get(String tag, String separator) {
		checkIfContextExists();
		return terms(tag).stream().map(Term::value).collect(Collectors.joining(separator));
	}

	public List<Term> terms() {
		checkIfContextExists();
		if (terms.isEmpty()) terms.addAll(context.terms(this));
		return terms;
	}

	public List<Term> terms(String tag) {
		checkIfContextExists();
		return terms().stream()
				.filter(t -> t.is(tag))
				.toList();
	}

	public Subject create(String name, String type) {
		Subject child = new Subject(this, name, type);
		return context != null ? context.create(child) : child;
	}

	public Subject open(String name, String type) {
		Subject subject = new Subject(this, name, type);
		return context.get(subject.identifier);
	}

	public Subject open(String identifier) {
		Subject subject = new Subject(this, identifier);
		return context.get(subject.identifier);
	}

	public Subject rename(String name) {
		Subject subject = new Subject(path() + name + "." + type(), context);
		context.rename(this, subject.identifier);
		return subject;
	}

	public void drop() {
		checkIfContextExists();
		context.drop(this);
	}

	public Indexing index() {
		checkIfContextExists();
		terms.clear();
		return context.index(this);
	}

	public boolean hasHistory() {
		return context.hasHistory(this);
	}

	public SubjectHistory history() {
		return context.history(this);
	}

	private Context context() {
		return context;
	}

	private SubjectQuery query() {
		return new SubjectQuery() {
			private final List<Predicate<Subject>> conditions = new ArrayList<>();

			@Override
			public int size() {
				return (int) subjects().count();
			}

			@Override
			public boolean isEmpty() {
				return subjects().findAny().isEmpty();
			}

			@Override
			public Subject first() {
				return subjects().findFirst().orElse(null);
			}

			@Override
			public Stream<Subject> stream() {
				return subjects();
			}

			@Override
			public List<Subject> collect() {
				return subjects().toList();
			}

			@Override
			public SubjectQuery type(String... types) {
				return type(Set.of(types));
			}

			private SubjectQuery type(Set<String> types) {
				conditions.add(s-> types.contains(s.type()));
				return this;
			}

			@Override
			public SubjectQuery isRoot() {
				conditions.add(Subject::isRoot);
				return this;
			}

			@Override
			public SubjectQuery with(String tag, String value) {
				conditions.add(contains(tag, value));
				return this;
			}

			@Override
			public SubjectQuery without(String tag, String value) {
				conditions.add(notContains(tag, value));
				return this;
			}

			@Override
			public AttributeFilter where(String... tags) {
				return attributeFilter(subjects());
			}

			private Stream<Subject> subjects() {
				return context.children(Subject.this)
						.stream()
						.filter(c->conditions().test(c));
			}

			private Predicate<Subject> conditions() {
				return conditions.stream().reduce(x -> true, Predicate::and);
			}

		};
	}

	private static Predicate<Subject> contains(Term term) {
		return s -> s.terms().contains(term);
	}

	private static Predicate<Subject> contains(String tag, String value) {
		return contains(new Term(tag, value));
	}

	private static Predicate<Subject> notContains(Term term) {
		return s -> !s.terms().contains(term);
	}

	private static Predicate<Subject> notContains(String tag, String value) {
		return notContains(new Term(tag, value));
	}

	private SubjectQuery.AttributeFilter attributeFilter(Stream<Subject> subjects) {
		return new SubjectQuery.AttributeFilter() {

			@Override
			public List<Subject> contains(String value) {
				return matches(v -> v.toLowerCase().contains(value.toLowerCase()));
			}

			@Override
			public List<Subject> accepts(String value) {
				return matches(t -> pattern(t).matcher(value).matches());
			}

			@Override
			public List<Subject> matches(Predicate<String> predicate) {
				return subjects
						.filter(s -> anyMatch(predicate, s.terms()))
						.toList();
			}

			private boolean anyMatch(Predicate<String> predicate, List<Term> terms) {
				return terms.stream().map(Term::value).anyMatch(predicate);
			}
		};
	}



	private String path() {
		return isRoot() ? "" : parentIdentifier() + "/";
	}

	private void checkIfContextExists() {
		if (context != Context.Null) return;
		System.err.println("Context is not defined for '" + identifier + "'");
	}

	@Override
	public String toString() {
		return identifier;
	}

	@Override
	public boolean equals(Object object) {
		return object instanceof Subject subject && Objects.equals(identifier, subject.identifier);
	}

	@Override
	public int hashCode() {
		return Objects.hash(identifier);
	}

	private String parentIdentifier() {
		int i = identifier.lastIndexOf('/');
		return i >= 0 ? identifier.substring(0, i) : "";
	}

	public interface Context {
		Context Null = nullContext();

		List<Subject> children(Subject subject);

		List<Term> terms(Subject subject);

		Subject create(Subject child);

		Subject get(String identifier);

		void rename(Subject subject, String identifier);

		void drop(Subject subject);

		Indexing index(Subject subject);

		SubjectHistory history(Subject subject);

		boolean hasHistory(Subject subject);
	}

	public interface Indexing {
		Indexing Null = nullIndexing();

		Indexing set(Term term);

		Indexing put(Term term);

		Indexing del(Term term);

		Indexing del(String tag);

		default Indexing set(String tag, Number value) {
			return set(new Term(tag, String.valueOf(value)));
		}

		default Indexing set(String tag, String value) {
			return set(new Term(tag, value));
		}

		default Indexing put(String tag, Number value) {
			return put(new Term(tag, String.valueOf(value)));
		}

		default Indexing put(String tag, String value) {
			return put(new Term(tag, value));
		}

		default Indexing del(String tag, String value) {
			return del(new Term(tag, value));
		}

		void terminate();
	}

	private static Context nullContext() {
		return new Context() {

			@Override
			public List<Subject> children(Subject subject) {
				return List.of();
			}

			@Override
			public List<Term> terms(Subject subject) {
				return List.of();
			}

			@Override
			public Indexing index(Subject subject) {
				return Indexing.Null;
			}

			@Override
			public Subject create(Subject child) {
				return child;
			}

			@Override
			public Subject get(String identifier) {
				return new Subject(identifier);
			}

			@Override
			public void rename(Subject subject, String name) {
			}

			@Override
			public void drop(Subject subject) {

			}

			@Override
			public SubjectHistory history(Subject subject) {
				return null;
			}

			@Override
			public boolean hasHistory(Subject subject) {
				return false;
			}
		};
	}

	private static Indexing nullIndexing() {
		return new Indexing() {

			@Override
			public Indexing set(Term term) {
				return this;
			}

			@Override
			public Indexing put(Term term) {
				return this;
			}

			@Override
			public Indexing del(Term term) {
				return this;
			}

			@Override
			public Indexing del(String tag) {
				return this;
			}

			@Override
			public void terminate() {

			}
		};
	}


}
