package systems.intino.datamarts.subjectstore.index.model;

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

	public Transaction update() {
		checkIfContextExists();
		return context.update(this);
	}

	public SubjectHistory history() {
		return context.history(this);
	}

	public Subject rename(String name) {
		//TODO no se modifica en la BBDD
		context.rename(name);
		String parentIdentifier = parentIdentifier();
		return parentIdentifier.isEmpty() ?
				new Subject(name + "." + type(), context) :
				new Subject(parentIdentifier + "/" + name + "." + type(), context);
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
		return parent().isNull();
	}

	public interface Context {
		Context Null = nullContext();
		Subjects children(Subject subject);
		Terms terms(Subject subject);

		Subject create(Subject child);
		void rename(String name);
		Transaction update(Subject subject);
		void drop(Subject subject);

		SubjectHistory history(Subject subject);
	}

	public interface Transaction {
		Transaction Null = nullTransaction();

		Transaction rename(String name);

		Transaction set(Term term);
		Transaction put(Term term);
		Transaction del(Term term);
		Transaction del(String tag);

		default Transaction set(String tag, double value) {
			return set(new Term(tag,String.valueOf(value)));
		}
		default Transaction set(String tag, String value) {
			return set(new Term(tag, value));
		}
		default Transaction put(String tag, double value) {
			return put(new Term(tag, String.valueOf(value)));
		}
		default Transaction put(String tag, String value) {
			return put(new Term(tag, value));
		}
		default Transaction del(String tag, String value) {
			return del(new Term(tag, value));
		}

		void commit();
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
			public Transaction update(Subject subject) {
				return Transaction.Null;
			}

			@Override
			public Subject create(Subject child) {
				return child;
			}

			@Override
			public void rename(String name) {

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

	private static Transaction nullTransaction() {
		return new Transaction() {
			@Override
			public Transaction rename(String name) {
				return this;
			}

			@Override
			public Transaction set(Term term) {
				return this;
			}

			@Override
			public Transaction put(Term term) {
				return this;
			}

			@Override
			public Transaction del(Term term) {
				return this;
			}

			@Override
			public Transaction del(String tag) {
				return this;
			}

			@Override
			public void commit() {

			}
		};
	}

}
