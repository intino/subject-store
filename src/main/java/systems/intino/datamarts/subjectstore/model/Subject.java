package systems.intino.datamarts.subjectstore.model;

import systems.intino.datamarts.subjectstore.SubjectHistory;

import java.util.List;
import java.util.Objects;

public record Subject(String identifier, Context context) {
	public static final String Any = "*";

	public static Subject of(String identifier) {
		if (identifier == null) return null;
		return new Subject(identifier);
	}

	public Subject {
		identifier = identifier != null ? identifier.trim() : "";
		context = context != null ? context : Context.Null;
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
		this(parent.identifier() + "/" + identifier, parent.context());
	}

	public Subject(Subject parent, String name, String type) {
		this(parent, name + "." + type);
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

	public Subject parent() {
		return new Subject(parentIdentifier(), context);
	}

	public Subjects children() {
		checkIfContextExists();
		return context.children(this);
	}

	public Terms terms() {
		checkIfContextExists();
		return context.terms(this);
	}

	public Subject create(String name, String type) {
		Subject child = new Subject(this, name, type);
		return context != null ? context.create(child) : child;
	}

	public Updating index() {
		checkIfContextExists();
		return context.update(this);
	}

	public SubjectHistory history() {
		return context.history(this);
	}

	public Subject rename(String name) {
		Subject subject = new Subject(path() + name + "." + type(), context);
		context.rename(this, subject.identifier());
		return subject;
	}

	private String path() {
		return isRoot() ? "" : parentIdentifier() + "/";
	}

	public void drop() {
		checkIfContextExists();
		context.drop(this);
	}

	private void checkIfContextExists() {
		if (context != Context.Null) return;
		System.err.println("Context is not defined for '" + identifier + "'");
	}

	public boolean is(String type) {
		return type.equals("*") || type.equals(this.type());
	}

	public boolean isNull() {
		return this.identifier.isEmpty();
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

	public boolean isRoot() {
		return identifier.lastIndexOf('/') < 0;
	}

	public interface Context {
		Context Null = nullContext();
		Subjects children(Subject subject);
		Terms terms(Subject subject);

		Subject create(Subject child);
		void rename(Subject subject, String identifier);
		void drop(Subject subject);

		Updating update(Subject subject);
		SubjectHistory history(Subject subject);
	}

	public interface Updating {
		Updating Null = nullTransaction();

		Updating set(Term term);
		Updating put(Term term);
		Updating del(Term term);
		Updating del(String tag);

		default Updating set(String tag, Number value) {
			return set(new Term(tag,String.valueOf(value)));
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
			public Subjects children(Subject subject) {
				return new Subjects(List.of());
			}

			@Override
			public Terms terms(Subject subject) {
				return new Terms(List.of());
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

	private static Updating nullTransaction() {
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
