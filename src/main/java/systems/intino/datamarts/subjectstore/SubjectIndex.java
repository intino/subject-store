package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.io.IndexRegistry;
import systems.intino.datamarts.subjectstore.io.Statements;
import systems.intino.datamarts.subjectstore.io.registries.SqlStorage;
import systems.intino.datamarts.subjectstore.io.statements.DumpStatements;
import systems.intino.datamarts.subjectstore.io.registries.SqlIndexRegistry;
import systems.intino.datamarts.subjectstore.model.*;
import systems.intino.datamarts.subjectstore.model.Subject.Context;
import systems.intino.datamarts.subjectstore.model.Subject.Updating;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectstore.model.Subject.Any;

public class SubjectIndex implements AutoCloseable {
	private final String storage;
	private final IndexRegistry registry;
	private final Lookup<Subject> subjects;
	private final Lookup<Term> terms;
	private final Context context;

	public SubjectIndex(String storage) {
		this.storage = storage;
		this.registry = new SqlIndexRegistry(storage);
		this.subjects = new Lookup<>(registry.subjects(), Subject::of, this::insert);
		this.terms = new Lookup<>(registry.terms(), Term::of, this::insert);
		this.context = context();
	}

	public boolean has(String name, String type) {
		return subjects.contains(new Subject(name, type));
	}

	public boolean has(String identifier) {
		return subjects.contains(new Subject(identifier));
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
		registry.commit();
		return get(id);
	}

	private void rename(Subject subject, String identifier) {
		int id = subjects.id(subject);
		subjects.set(id, new Subject(identifier));
		registry.setSubject(id, identifier);
		registry.commit();
	}

	private Updating update(Subject subject) {
		return new Updating() {
			private final int id = subjects.id(subject);
			private final List<Integer> currentTerms = registry.termsOf(id);
			private final List<Integer> exclusiveTerms = registry.exclusiveTermsOf(id);

			@Override
			public Updating set(Term term) {
				int[] candidateTerms = termsWith(term.tag());
				return candidateTerms.length == 1 ?
						exclusiveTerms.contains(candidateTerms[0]) ?
							modifyTerm(term, candidateTerms[0]) :
							replaceTerm(term, candidateTerms[0]) :
						replaceTerms(term, candidateTerms);
			}

			private Updating modifyTerm(Term term, int id) {
				registry.setTerm(id, term.toString());
				terms.set(id, term);
				return this;
			}

			private Updating replaceTerm(Term term, int id) {
				registry.unlink(this.id, id);
				return link(term);
			}

			private Updating replaceTerms(Term term, int[] candidateTerms) {
				del(candidateTerms);
				return link(term);
			}

			public Updating put(Term term) {
				return link(term);
			}

			private Updating link(Term term) {
				int id = terms.add(term);
				registry.link(this.id, id);
				return this;
			}

			@Override
			public Updating del(String tag) {
				int[] candidateTerms = termsWith(tag);
				return del(candidateTerms);
			}

			@Override
			public Updating del(Term term) {
				return del(id(term));
			}

			private Updating del(int id) {
				if (id < 0) return this;
				registry.unlink(this.id, id);
				if (exclusiveTerms.contains(id)) {
					exclusiveTerms.remove((Integer) id);
					terms.set(id, null);
					registry.deleteTerm(id);
				}
				currentTerms.remove((Integer) id);
				return this;
			}

			private Updating del(int[] terms) {
				for (int term : terms) del(term);
				return this;
			}

			private int[] termsWith(String tag) {
				return currentTerms.stream()
						.filter(t -> term(t).is(tag))
						.mapToInt(t->t)
						.toArray();
			}

			private int id(Term term) {
				return terms.id(term);
			}

			private Term term(int term) {
				return terms.get(term);
			}

			@Override
			public void terminate() {
				registry.commit();
			}
		};
	}

	private void drop(Subject subject) {
		if (isNotValid(subject)) return;
		dropChildrenOf(subject);
		dropHistoryOf(subject);
		dropIndexOf(subject);
	}

	private boolean isNotValid(Subject subject) {
		return subject.isNull() || !subjects.contains(subject);
	}

	private void dropChildrenOf(Subject subject) {
		subject.children().forEach(this::drop);
	}

	private void dropIndexOf(Subject subject) {
		int id = subjects.id(subject);
		terms.remove(registry.exclusiveTermsOf(id));
		subjects.remove(subject);
		registry.deleteSubject(id);
	}

	private void dropHistoryOf(Subject subject) {
		try (SubjectHistory history = subject.history()) {
			history.drop();
		}
	}

	private static final SubjectQuery.SubjectFilter EmptyQuery = emptyQuery();
	private static SubjectQuery.SubjectFilter emptyQuery() {
		return new SubjectQuery.SubjectFilter() {

			@Override
			public SubjectQuery.SubjectFilter with(Term term) {
				return this;
			}

			@Override
			public SubjectQuery.SubjectFilter without(Term term) {
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

	private SubjectQuery.SubjectFilter subjectFilter(Set<String> types) {
		return new SubjectQuery.SubjectFilter() {
			private final List<Integer> candidates = subjectsWith(types);
			private final List<Integer> condition = new ArrayList<>();

			@Override
			public SubjectQuery.SubjectFilter with(Term term) {
				if (!terms.contains(term)) return EmptyQuery;
				condition.add(terms.id(term));
				return this;
			}

			@Override
			public SubjectQuery.SubjectFilter without(Term term) {
				if (terms.contains(term))
					condition.add(-terms.id(term));
				return this;
			}

			@Override
			public Subject first() {
				return retrieve(s->true).get(0);
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
				List<Integer> search = condition.isEmpty() ? candidates : registry.subjectsFilteredBy(candidates, condition);
				List<Subject> subjects = toSubjects(search).filter(predicate).toList();
				return new Subjects(subjects);
			}
		};
	}

	private SubjectQuery.AttributeFilter attributeFilter(Set<String> types, Set<String> keys) {
		return new SubjectQuery.AttributeFilter() {
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
			public Subjects accepts(String value) {
				List<Integer> terms = termsAcceptedBy(value);
				return subjectSetWith(terms);
			}

			@Override
			public Subjects matches(Predicate<String> predicate) {
				List<Integer> terms = termsMatchedBy(predicate);
				return subjectSetWith(terms);
			}

			private Subjects subjectSetWith(List<Integer> terms) {
				List<Subject> subjects = terms.isEmpty() ? List.of() : toSubjects(registry.subjectsFilteredBy(candidates, terms)).toList();
				return new Subjects(subjects);
			}

			private List<Integer> termsAcceptedBy(String value) {
				return terms(keys)
						.filter(t -> pattern(t.value()).matcher(value).matches())
						.map(terms::id)
						.toList();
			}

			private List<Integer> termsMatchedBy(Predicate<String> predicate) {
				return terms(keys)
						.filter(t->predicate.test(t.value()))
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

	public Statements statements() {
		return this::stamentIterator;
	}

	public SubjectIndex consume(Statements statements) {
		Batch batch = batch();
		for (Statement statement : statements)
			batch.put(statement);
		batch.terminate();
		return this;
	}

	public void dump(OutputStream os) {
		statements().forEach(s->write(s.toString() + '\n', os));
	}

	public SubjectIndex restore(InputStream is) throws IOException {
		try (DumpStatements statements = new DumpStatements(is)) {
			return consume(statements);
		}
	}

	private Iterator<Statement> stamentIterator() {
		return registry.dump().map(Statement::new).iterator();
	}

	private void write(String str, OutputStream os) {
		try {
			os.write(str.getBytes());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private Context context() {
		return new Context() {
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
				return new Terms(id < 0 ? List.of() : toTerms(registry.termsOf(id)));
			}

			private List<Term> toTerms(List<Integer> terms) {
				return terms.stream().map(SubjectIndex.this.terms::get).toList();
			}

			@Override
			public Subject create(Subject subject) {
				return SubjectIndex.this.create(subject.identifier());
			}

			@Override
			public void rename(Subject subject, String identifier) {
				SubjectIndex.this.rename(subject, identifier);
			}

			@Override
			public Updating update(Subject subject) {
				return SubjectIndex.this.update(subject);
			}

			@Override
			public void drop(Subject subject) {
				SubjectIndex.this.drop(subject);
			}

			@Override
			public SubjectHistory history(Subject subject) {
				return new SubjectHistory(subject.identifier(), SqlStorage.shared(storage));
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
		return new Subject(subject, context);
	}

	private int insert(Subject subject) {
		int index = subjects.indexOfNull();
		if (index <= 0) return registry.insertSubject(subject.identifier());
		registry.setTerm(index, subject.identifier());
		return index;
	}

	private int insert(Term term) {
		int index = terms.indexOfNull();
		if (index <= 0) return registry.insertTerm(term.toString());
		registry.setTerm(index, term.toString());
		return index;
	}

	private Stream<Subject> toSubjects(List<Integer> subjects) {
		return subjects.stream()
				.map(this.subjects::get)
				.map(this::wrap);
	}

	public Batch batch() {
		return new Batch() {
			@Override
			public void put(Subject subject, Term term) {
				int subjectId = subjects.add(subject);
				int termId = terms.add(term);
				registry.link(subjectId, termId);
			}

			@Override
			public void terminate() {
				registry.commit();
			}
		};
	}

	@Override
	public void close() throws Exception {
		registry.close();
	}

	public interface Batch {

		default void put(Statement statement) {
			put(statement.subject(), statement.term());
		}

		void put(Subject subject, Term term);

		default void put(Subject subject, String tag, String value) {
			put(subject, new Term(tag, value));
		}

		void terminate();
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
			if (id - 1 == list.size())
				list.add(t);
			else
				list.set(id - 1, t);
			map.put(t, id);
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

		public int indexOfNull() {
			return list.indexOf(null) + 1;
		}
	}


}
