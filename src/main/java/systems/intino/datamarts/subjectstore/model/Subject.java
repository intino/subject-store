package systems.intino.datamarts.subjectstore.model;

import systems.intino.datamarts.subjectstore.SubjectQuery;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record Subject(String identifier, Context context) {

	public Subject {
		identifier = clean(identifier);
	}

	public static Subject of(String identifier, Context context) {
		return identifier != null ? new Subject(identifier, context) : null;
	}

	public static Subject of(String name, String type) {
		return new Subject(identifier(name, type), Context.Null);
	}

	public static Subject of(String name, String type, Context context) {
		return of(identifier(name, type), context);
	}

	public static Subject of(String identifier) {
		return identifier != null ? new Subject(identifier, Context.Null) : null;
	}

	private static String clean(String identifier) {
		if (identifier == null) return null;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < identifier.length(); i++) {
			char c = identifier.charAt(i);
			if (Character.isWhitespace(c)) continue;
			sb.append(c);
		}
		return sb.toString();
	}

	public Subject(Subject parent, String identifier) {
		this(parent.identifier + "/" + identifier, parent.context);
	}

	public Subject(Subject parent, String name, String type) {
		this(parent, identifier(name, type));
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
		return type.equals(this.type());
	}

	public boolean isNull() {
		return this.identifier.isEmpty();
	}

	public boolean isRoot() {
		return identifier.lastIndexOf('/') < 0;
	}

	public boolean isChildOf(Subject subject) {
		return isChildOf(subject.identifier);
	}

	public boolean isChildOf(String identifier) {
		return parentIdentifier().equals(identifier);
	}

	public boolean isUnderOf(Subject subject) {
		return isUnderOf(subject.identifier);
	}

	public boolean isUnderOf(String identifier) {
		return parentIdentifier().startsWith(identifier);
	}

	public Subject parent() {
		return new Subject(parentIdentifier(), context);
	}

	public SubjectQuery children() {
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

	public Updating update() {
		checkIfContextExists();
		return context.update(this);
	}

	private SubjectQuery query() {
		return new SubjectQuery() {
			private final SubjectQuery This = this;
			private final List<Predicate<Subject>> conditions = new ArrayList<>();
			private final Sorting sorting = new Sorting();

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
				return subjects().sorted(sorting::sort);
			}

			@Override
			public List<Subject> collect() {
				return subjects().toList();
			}

			@Override
			public SubjectQuery isType(String type) {
				conditions.add(s-> s.is(type));
				return this;
			}

			@Override
			public SubjectQuery isRoot() {
				conditions.add(Subject::isRoot);
				return this;
			}

			@Override
			public SubjectQuery isChildOf(String identifier) {
				conditions.add(s -> s.isChildOf(identifier));
				return this;
			}

			@Override
			public SubjectQuery isUnderOf(String identifier) {
				conditions.add(s -> s.isUnderOf(identifier));
				return this;
			}

			@Override
			public SubjectQuery orderBy(String tag, Comparator<String> comparator) {
				sorting.add(tag, comparator);
				return this;
			}

			@Override
			public AttributeFilter where(String tag) {
				return predicate -> where(tag, predicate);
			}

			private SubjectQuery where(String tag, Predicate<String> predicate) {
				conditions.add(s->s.terms().stream().anyMatch(t-> t.is(tag) && predicate.test(t.value())));
				return This;
			}

			private Stream<Subject> subjects() {
				return subjectsWith(conditions());
			}

			private Stream<Subject> subjectsWith(Predicate<Subject> conditions) {
				return context.children(Subject.this)
						.stream()
						.filter(conditions);
			}

			private Predicate<Subject> conditions() {
				return conditions.stream().reduce(x -> true, Predicate::and);
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

		Updating update(Subject subject);

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

	}

	private static Context nullContext() {
		return new Context() {
			private final List<Subject> subjects = new ArrayList<>();

			@Override
			public List<Subject> children(Subject subject) {
				return subjects.stream().filter(s->s.isChildOf(subject)).toList();
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
				subjects.add(child);
				return child;
			}

			@Override
			public Subject get(String identifier) {
				return new Subject(identifier, Context.Null);
			}

			@Override
			public void rename(Subject subject, String name) {
			}

			@Override
			public void drop(Subject subject) {

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

		};
	}

	private static String identifier(String name, String type) {
		return name + "." + type;
	}

}
