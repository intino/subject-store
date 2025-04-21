package systems.intino.datamarts.subjectstore.model;

import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectQuery;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectstore.model.PatternFactory.pattern;

public final class Subject {
	public static final String Any = "*";
	private final String identifier;
	private final Context context;

	public static Subject of(String identifier) {
		if (identifier == null) return null;
		return new Subject(identifier);
	}

	public Subject(String identifier, Context context) {
		this.identifier = identifier != null ? identifier.trim().replaceAll("\\s+/\\s+", "/") : null;;
		this.context = context != null ? context : Context.Null;;
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
		return context.terms(this);
	}

	public List<Term> terms(String tag) {
		checkIfContextExists();
		return context.terms(this).stream()
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

	public Updating index() {
		checkIfContextExists();
		return context.update(this);
	}

	public SubjectHistory history() {
		return context.history(this);
	}

	private Context context() {
		return context;
	}

	private SubjectQuery query() {
		return new SubjectQuery() {
			private final Set<String> types = new HashSet<>();

			@Override
			public int size() {
				return subjects().size();
			}

			@Override
			public Subject first() {
				return subjects().getFirst();
			}

			@Override
			public List<Subject> collect() {
				return subjects();
			}

			@Override
			public SubjectQuery type(String... types) {
				this.types.addAll(Arrays.asList(types));
				return this;
			}

			@Override
			public SubjectFilter roots() {
				return subjectFilter(subjects()).that(Subject::isRoot);
			}

			@Override
			public SubjectFilter with(String tag, String value) {
				return subjectFilter(subjects()).that(contains(tag, value));
			}

			@Override
			public SubjectFilter without(String tag, String value) {
				return subjectFilter(subjects()).that(notContains(tag, value));
			}

			@Override
			public SubjectFilter that(Predicate<Subject> predicate) {
				return subjectFilter(subjects()).that(predicate);
			}

			@Override
			public AttributeFilter where(String... tags) {
				return attributeFilter(subjects());
			}

			private List<Subject> subjects() {
				return context.children(Subject.this, types);
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

	private SubjectQuery.SubjectFilter subjectFilter(List<Subject> subjects) {
		return new SubjectQuery.SubjectFilter() {
			private final List<Predicate<Subject>> conditions = new ArrayList<>();

			@Override
			public int size() {
				return (int) subjectStream().count();
			}

			@Override
			public Subject first() {
				return subjectStream().findFirst().orElse(null);
			}

			@Override
			public List<Subject> collect() {
				return subjectStream().toList();
			}

			@Override
			public SubjectQuery.SubjectFilter isRoot() {
				conditions.add(Subject::isRoot);
				return this;
			}

			@Override
			public SubjectQuery.SubjectFilter with(Term term) {
				conditions.add(contains(term));
				return this;
			}

			@Override
			public SubjectQuery.SubjectFilter without(Term term) {
				conditions.add(notContains(term));
				return this;
			}

			@Override
			public SubjectQuery.SubjectFilter that(Predicate<Subject> condition) {
				conditions.add(condition);
				return this;
			}

			private Predicate<Subject> conditions() {
				return conditions.stream().reduce(x -> true, Predicate::and);
			}

			private Stream<Subject> subjectStream() {
				return subjects.stream().filter(conditions());
			}
		};
	}

	private SubjectQuery.AttributeFilter attributeFilter(List<Subject> subjects) {
		return new SubjectQuery.AttributeFilter() {

			@Override
			public List<Subject> contains(String value) {
				return that(v -> v.toLowerCase().contains(value.toLowerCase()));
			}

			@Override
			public List<Subject> accepts(String value) {
				return that(t -> pattern(t).matcher(value).matches());
			}

			@Override
			public List<Subject> that(Predicate<String> predicate) {
				return subjects.stream()
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

		List<Subject> children(Subject subject, Set<String> types);

		List<Term> terms(Subject subject);

		Subject create(Subject child);

		Subject get(String identifier);

		void rename(Subject subject, String identifier);

		void drop(Subject subject);

		Updating update(Subject subject);

		SubjectHistory history(Subject subject);
	}

	public interface Updating {
		Updating Null = nullUpdating();

		Updating set(Term term);

		Updating put(Term term);

		Updating del(Term term);

		Updating del(String tag);

		default Updating set(String tag, Number value) {
			return set(new Term(tag, String.valueOf(value)));
		}

		default Updating set(String tag, String value) {
			return set(new Term(tag, value));
		}

		default Updating put(String tag, Number value) {
			return put(new Term(tag, String.valueOf(value)));
		}

		default Updating put(String tag, String value) {
			return put(new Term(tag, value));
		}

		default Updating del(String tag, String value) {
			return del(new Term(tag, value));
		}

		void terminate();
	}

	private static Context nullContext() {
		return new Context() {

			@Override
			public List<Subject> children(Subject subject, Set<String> types) {
				return List.of();
			}

			@Override
			public List<Term> terms(Subject subject) {
				return List.of();
			}

			@Override
			public Updating update(Subject subject) {
				return Updating.Null;
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
		};
	}

	private static Updating nullUpdating() {
		return new Updating() {

			@Override
			public Updating set(Term term) {
				return this;
			}

			@Override
			public Updating put(Term term) {
				return this;
			}

			@Override
			public Updating del(Term term) {
				return this;
			}

			@Override
			public Updating del(String tag) {
				return this;
			}

			@Override
			public void terminate() {

			}
		};
	}


}
