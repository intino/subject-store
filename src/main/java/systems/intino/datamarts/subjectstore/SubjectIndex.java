package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.index.io.IndexRegistry;
import systems.intino.datamarts.subjectstore.index.io.Statements;
import systems.intino.datamarts.subjectstore.index.io.statements.DumpStatements;
import systems.intino.datamarts.subjectstore.index.io.registries.SqlIndexRegistry;
import systems.intino.datamarts.subjectstore.index.model.*;
import systems.intino.datamarts.subjectstore.index.model.Subject.Transaction;

import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectstore.index.model.Subject.Any;

public class SubjectIndex implements Closeable {
	private final Connection connection;
	private final IndexRegistry indexRegistry;
	private final Lookup<Subject> subjects;
	private final Lookup<Term> terms;

	public SubjectIndex(File file) {
		this(SqliteConnection.from(file));
	}

	public SubjectIndex() {
		this(SqliteConnection.inMemory());
	}

	public SubjectIndex(Connection connection) {
		this.connection = connection;
		this.indexRegistry = new SqlIndexRegistry(connection);
		this.subjects = new Lookup<>(indexRegistry.subjects(), Subject::of, this::insert);
		this.terms = new Lookup<>(indexRegistry.terms(), Term::of, this::insert);
	}

	public Subject get(String name, String type) {
		return get(new Subject(name, type));
	}

	public Subject get(String identifier) {
		return get(Subject.of(identifier));
	}

	private Subject get(Subject subject) {
		return subjects.contains(subject) ? wrap(subject) : null;
	}

	private Subject get(int subject) {
		return subjects.contains(subject) ? wrap(subjects.get(subject)) : null;
	}

	public Terms terms() {
		return new Terms(terms.stream().toList());
	}

	public SubjectQuery subjects() {
		return subjects(Any);
	}

	public SubjectQuery subjects(String... types) {
		return subjects(setOf(types));
	}

	public SubjectQuery subjects(Set<String> types) {
		return new SubjectQuery() {

			@Override
			public Subjects all() {
				return subjectFilter(types).all();
			}

			@Override
			public Subjects roots() {
				return subjectFilter(types).roots();
			}

			@Override
			public AttributeFilter where(String... keys) {
				return attributeFilter(types, Set.of(keys));
			}

			@Override
			public SubjectFilter with(String tag, String value) {
				return subjectFilter(types).with(tag, value);
			}

			@Override
			public SubjectFilter without(String tag, String value) {
				return subjectFilter(types).without(tag, value);
			}
		};	}

	private Set<String> setOf(String[] types) {
		return Set.of(types);
	}

	public Subject create(String name, String type) {
		return create(new Subject(name, type));
	}

	public Subject create(String identifier) {
		return create(Subject.of(identifier));
	}

	private Subject create(Subject subject) {
		int id = subjects.add(subject);
		indexRegistry.commit();
		return get(id);
	}

	private Transaction update(Subject subject) {
		return new Transaction() {
			private final int id = subjects.id(subject);
			private final List<Integer> currentTerms = indexRegistry.termsOf(id);

			@Override
			public Transaction rename(String identifier) {
				if (identifier == null || identifier.isEmpty()) return this;
				return replace(subject.rename(identifier));
			}

			@Override
			public Transaction set(Term term) {
				del(term.tag());
				put(term);
				return this;
			}

			public Transaction put(Term term) {
				indexRegistry.link(id, terms.add(term));
				return this;
			}

			@Override
			public Transaction del(Term term) {
				indexRegistry.unlink(id, terms.add(term));
				return this;
			}

			@Override
			public Transaction del(String tag) {
				toTerms(currentTerms).filter(t -> t.is(tag)).forEach(this::del);
				return this;
			}

			private Transaction replace(Subject subject) {
				if (subjects.contains(subject)) return this;
				subjects.set(id, subject);
				indexRegistry.rename(id, subject.toString());
				return this;
			}

			@Override
			public void commit() {
				indexRegistry.commit();
			}
		};
	}

	private void drop(Subject subject) {
		if (subject.isNull() || !subjects.contains(subject)) return;
		subject.children().forEach(this::drop);
		int id = subjects.id(subject);
		terms.remove(indexRegistry.exclusiveTermsOf(id));
		indexRegistry.drop(id);
		subjects.remove(subject);
	}

	private SubjectFilter subjectFilter(Set<String> types) {
		return new SubjectFilter() {
			private final List<Integer> candidates = subjectsWith(types);
			private final List<Integer> condition = new ArrayList<>();

			@Override
			public SubjectFilter with(Term term) {
				if (!terms.contains(term)) return SubjectFilter.Empty;
				condition.add(terms.id(term));
				return this;
			}

			@Override
			public SubjectFilter without(Term term) {
				if (terms.contains(term))
					condition.add(-terms.id(term));
				return this;
			}

			@Override
			public Subjects all() {
				return retrieve(s -> true);
			}

			@Override
			public Subjects roots() {
				return retrieve(s -> s.parent().isNull());
			}

			private Subjects retrieve(Predicate<Subject> predicate) {
				List<Integer> search = condition.isEmpty() ? candidates : indexRegistry.subjectsFilteredBy(candidates, condition);
				List<Subject> subjects = toSubjects(search).filter(predicate).toList();
				return new Subjects(subjects);
			}
		};
	}

	private AttributeFilter attributeFilter(Set<String> types, Set<String> keys) {
		return new AttributeFilter() {
			private final List<Integer> candidates = subjectsWith(types);

			@Override
			public Subjects contains(String value) {
				String[] values = value.split(" ");
				return subjectSetWith(termsWith(values));
			}

			private List<Integer> termsWith(String[] values) {
				List<List<Integer>> lists = Arrays.stream(values)
						.filter(v -> !v.isEmpty())
						.map(this::termsContaining)
						.collect(Collectors.toList());
				return join(lists);
			}

			private static List<Integer> join(List<List<Integer>> lists) {
				if (lists.isEmpty()) return List.of();
				Set<Integer> join = new HashSet<>(lists.getFirst());
				IntStream.range(1, lists.size())
						.mapToObj(lists::get)
						.forEach(join::retainAll);
				return new ArrayList<>(join);
			}
			@Override
			public Subjects matches(String value) {
				List<Integer> terms = termsFitting(value);
				return subjectSetWith(terms);
			}

			private Subjects subjectSetWith(List<Integer> terms) {
				List<Subject> subjects = terms.isEmpty() ? List.of() : toSubjects(indexRegistry.subjectsFilteredBy(candidates, terms)).toList();
				return new Subjects(subjects);
			}

			private List<Integer> termsFitting(String value) {
				return terms(keys)
						.filter(t -> pattern(t.value()).matcher(value).matches())
						.map(terms::id)
						.toList();
			}

			private List<Integer> termsContaining(String value) {
				return terms(keys)
						.filter(t -> t.value().contains(value))
						.map(terms::id)
						.toList();
			}

			private Stream<Term> terms(Set<String> keys) {
				return terms.stream()
						.filter(t -> keys.contains(t.tag()));
			}
		};
	}

	public static final double nullThresholdRatio = 0.20;
	public boolean isFragmented() {
		return subjects.nullRatio() > nullThresholdRatio || terms.nullRatio() > nullThresholdRatio;
	}

	public Statements statements() {
		return this::stamentIterator;
	}

	public SubjectIndex consume(Statements statements) {
		Batch batch = batch();
		for (Statement statement : statements)
			batch.put(statement);
		batch.commit();
		return this;
	}

	public void copyTo(SubjectIndex subjectIndex) {
		subjectIndex.consume(this.statements());
	}

	public void dump(OutputStream os) {
		indexRegistry.dump().forEach(s->write(s + '\n', os));
	}

	public SubjectIndex restore(InputStream is) throws IOException {
		try (DumpStatements statements = new DumpStatements(is)) {
			return consume(statements);
		}
	}

	private Iterator<Statement> stamentIterator() {
		return indexRegistry.dump().map(Statement::new).iterator();
	}

	private void write(String str, OutputStream os) {
		try {
			os.write(str.getBytes());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		indexRegistry.close();
	}

	private Subject.Context context() {
		return new Subject.Context() {
			@Override
			public Subjects children(Subject subject) {
				return new Subjects(subjects.stream()
						.filter(s -> s.parent().equals(subject))
						.map(s->wrap(s))
						.toList());
			}

			@Override
			public Terms terms(Subject subject) {
				int id = subjects.id(subject);
				return new Terms(id < 0 ? List.of() : toTerms(indexRegistry.termsOf(id)).toList());
			}

			@Override
			public Subject create(Subject subject) {
				return SubjectIndex.this.create(subject.identifier());
			}

			@Override
			public void rename(String name) {
				//TODO
			}

			@Override
			public Transaction update(Subject subject) {
				return SubjectIndex.this.update(subject);
			}

			@Override
			public void drop(Subject subject) {
				SubjectIndex.this.drop(subject);
			}

			@Override
			public SubjectHistory history(Subject subject) {
					return new SubjectHistory(subject.identifier(), connection);
			}
		};
	}

	private List<Integer> subjectsWith(Set<String> types) {
		return subjectsWith(predicateFor(types));
	}

	private List<Integer> subjectsWith(Predicate<Subject> predicate) {
		return subjects.stream()
				.filter(predicate)
				.map(subjects::id)
				.toList();
	}

	private Predicate<Subject> predicateFor(Set<String> types) {
		return types.contains(Any) ? s->true : s -> types.contains(s.type());
	}

	private final Map<String, Pattern> patterns = new HashMap<>();

	private Pattern pattern(String value) {
		return patterns.computeIfAbsent(value, s -> Pattern.compile(value));
	}

	private Subject wrap(Subject subject) {
		return new Subject(subject, context());
	}

	private int insert(Subject subject) {
		return indexRegistry.insertSubject(subject.identifier());
	}

	private int insert(Term term) {
		return indexRegistry.insertTerm(term.toString());
	}

	private Stream<Subject> toSubjects(List<Integer> subjects) {
		return subjects.stream()
				.map(this.subjects::get)
				.map(this::wrap);
	}

	private Stream<Term> toTerms(List<Integer> terms) {
		return terms.stream().map(this.terms::get);
	}

	public Batch batch() {
		return new Batch() {
			@Override
			public void put(Subject subject, Term term) {
				int subjectId = subjects.add(subject);
				int termId = terms.add(term);
				indexRegistry.link(subjectId, termId);
			}

			@Override
			public void commit() {
				indexRegistry.commit();
			}
		};
	}

	public interface Batch {

		default void put(Statement statement) {
			put(statement.subject(), statement.term());
		}

		void put(Subject subject, Term term);

		default void put(Subject subject, String tag, String value) {
			put(subject, new Term(tag, value));
		}

		void commit();
	}

	public interface SubjectQuery {
		Subjects all();
		Subjects roots();
		SubjectFilter with(String tag, String value);
		SubjectFilter without(String tag, String value);
		AttributeFilter where(String... keys);
	}

	public interface SubjectFilter {
		Subjects all();
		Subjects roots();

		SubjectFilter Empty = emptyQuery();
		SubjectFilter with(Term term);
		SubjectFilter without(Term term);

		default SubjectFilter with(String tag, String value) {
			return with(new Term(tag, value));
		}
		default SubjectFilter without(String tag, String value) {
			return without(new Term(tag, value));
		}
	}

	public interface AttributeFilter {
		Subjects contains(String value);
		Subjects matches(String value);
	}

	private static class Lookup<T> {
		private final List<T> list;
		private final Function<T, Integer> idStore;
		private final Map<T, Integer> map;

		public Lookup(List<String> list, Function<String,T> deserializer, Function<T, Integer> idStore) {
			this.list = new ArrayList<>(list.stream().map(deserializer).toList());
			this.idStore = idStore;
			this.map = init(new HashMap<>());
		}

		private Map<T, Integer> init(Map<T, Integer> map) {
			for (int id = 1; id <= list.size(); id++)
				map.put(get(id), id);
			return map;
		}

		public int id(T t) {
			return contains(t) ? map.get(t) : -1;
		}

		public T get(int id) {
			return list.get(id - 1);
		}

		public boolean contains(T t) {
			return map.containsKey(t);
		}

		public int add(T t) {
			if (contains(t)) return map.get(t);
			int id = idStore.apply(t);
			list.add(t);
			map.put(t, id);
			assert id == list.size();
			return id;
		}

		public void remove(T t) {
			if (!contains(t)) return;
			remove(id(t));
		}

		public void remove(int id) {
			map.remove(get(id));
			list.set(id - 1, null);
		}

		public void remove(List<Integer> terms) {
			terms.forEach(this::remove);
		}

		public Stream<T> stream() {
			return list.stream().filter(Objects::nonNull);
		}

		public void set(int id, T t) {
			map.remove(get(id));
			map.put(t, id);
			list.set(id - 1, t);
		}

		public boolean contains(int id) {
			return id <= list.size();
		}

		public double nullRatio() {
			return list.size() > 20 ? (double) nullItems() / list.size() : 0;
		}

		private long nullItems() {
			return list.stream().filter(Objects::isNull).count();
		}
	}

	private static SubjectFilter emptyQuery() {
		return new SubjectFilter() {

			@Override
			public SubjectFilter with(Term term) {
				return this;
			}

			@Override
			public SubjectFilter without(Term term) {
				return this;
			}

			@Override
			public Subjects roots() {
				return new Subjects(List.of());
			}

			@Override
			public Subjects all() {
				return roots();
			}
		};
	}
}
