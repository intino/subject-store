package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.io.IndexRegistry;
import systems.intino.datamarts.subjectstore.io.Statements;
import systems.intino.datamarts.subjectstore.io.registries.SqlConnection;
import systems.intino.datamarts.subjectstore.io.statements.DumpStatements;
import systems.intino.datamarts.subjectstore.io.registries.SqlIndexRegistry;
import systems.intino.datamarts.subjectstore.model.*;
import systems.intino.datamarts.subjectstore.model.Subject.Context;
import systems.intino.datamarts.subjectstore.model.Subject.Indexing;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectstore.helpers.PatternFactory.pattern;
import static systems.intino.datamarts.subjectstore.model.Subject.Any;

public class SubjectIndex implements AutoCloseable {
	private final String jdbcUrl;
	private final IndexRegistry registry;
	private final Lookup<Subject> subjects;
	private final Lookup<Term> terms;
	private final Context context;

	public SubjectIndex(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
		this.registry = new SqlIndexRegistry(jdbcUrl);
		this.subjects = new Lookup<>(registry.subjects(), Subject::of, this::insert);
		this.terms = new Lookup<>(registry.terms(), Term::of, this::insert);
		this.context = createContext();
	}

	public boolean has(String name, String type) {
		return subjects.contains(new Subject(name, type));
	}

	public boolean has(String identifier) {
		return subjects.contains(new Subject(identifier));
	}

	public Subject open(String name, String type) {
		return open(new Subject(name, type));
	}

	public Subject open(String identifier) {
		return open(Subject.of(identifier));
	}

	private Subject open(Subject subject) {
		return subjects.contains(subject) ? wrap(subject) : null;
	}

	private Subject open(int subject) {
		return subjects.contains(subject) ? wrap(subjects.get(subject)) : null;
	}

	public List<Term> terms() {
		return terms.stream().toList();
	}

	public SubjectQuery subjects() {
		return new SubjectQuery() {
			private final Set<String> types = new HashSet<>();
			private final List<Integer> tags = new ArrayList<>();
			private final List<Predicate<Integer>> conditions = new ArrayList<>();

			@Override
			public int size() {
				return (int) stream().count();
			}

			@Override
			public boolean isEmpty() {
				return stream().findAny().isEmpty();
			}

			@Override
			public Subject first() {
				return stream().findFirst().orElse(null);
			}

			@Override
			public Stream<Subject> stream() {
				return candidates().stream().map(i->toSubject(i));
			}

			@Override
			public List<Subject> collect() {
				return stream().toList();
			}

			@Override
			public SubjectQuery type(String... types) {
				this.types.addAll(Arrays.asList(types));
				return this;
			}

			@Override
			public SubjectQuery isRoot() {
				conditions.add(i->toSubject(i).isRoot());
				return this;
			}

			@Override
			public SubjectQuery with(String tag, String value) {
				Term term = new Term(tag, value);
				if (!terms.contains(term)) return SubjectQuery.Empty;
				tags.add(terms.id(term));
				return this;
			}

			@Override
			public SubjectQuery without(String tag, String value) {
				Term term = new Term(tag, value);
				if (terms.contains(term)) tags.add(-terms.id(term));
				return this;
			}

			@Override
			public AttributeFilter where(String... tags) {
				return attributeFilter(candidates(), Set.of(tags));
			}

			private List<Integer> candidates() {
				List<Integer> candidates = new ArrayList<>(subjectsWith(types).toList());
				candidates.retainAll(subjectsWithTags(candidates));
				candidates.retainAll(subjectsWithConditions(candidates));
				return candidates;
			}

			private List<Integer> subjectsWithConditions(List<Integer> candidates) {
				return conditions.isEmpty() ? candidates : candidates.stream().filter(conditions()).toList();
			}

			private List<Integer> subjectsWithTags(List<Integer> candidates) {
				return tags.isEmpty() ? candidates : registry.subjectsWithAll(tags);
			}

			private Predicate<Integer> conditions() {
				return conditions.stream().reduce(x -> true, Predicate::and);
			}
		};
	}

	private SubjectQuery.AttributeFilter attributeFilter(List<Integer> candidates, Set<String> tags) {
		return new SubjectQuery.AttributeFilter() {

			@Override
			public List<Subject> contains(String value) {
				String[] values = value.split(" ");
				return subjectsWith(termsWith(values));
			}

			@Override
			public List<Subject> accepts(String value) {
				return subjectsWith(termsAcceptedBy(value));
			}

			@Override
			public List<Subject> matches(Predicate<String> predicate) {
				return subjectsWith(termsMatchedBy(predicate));
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

			private List<Subject> subjectsWith(List<Integer> terms) {
				List<Integer> subjects = terms.isEmpty() ? List.of() : registry.subjectsWithAny(terms);
				candidates.retainAll(subjects);
				return toSubjects(candidates);
			}

			private List<Integer> termsAcceptedBy(String value) {
				return terms(tags)
						.filter(t -> pattern(t.value()).matcher(value).matches())
						.map(terms::id)
						.toList();
			}

			private List<Integer> termsMatchedBy(Predicate<String> predicate) {
				return terms(tags)
						.filter(t->predicate.test(t.value()))
						.map(terms::id)
						.toList();
			}

			private List<Integer> termsContaining(String value) {
				return terms(tags)
						.filter(t -> t.value().contains(value))
						.map(terms::id)
						.toList();
			}

			private Stream<Term> terms(Set<String> tags) {
				return terms.stream().filter(t -> tags.contains(t.tag()));
			}
		};
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
		return open(id);
	}

	private void rename(Subject subject, String identifier) {
		int id = subjects.id(subject);
		subjects.set(id, new Subject(identifier));
		registry.setSubject(id, identifier);
		registry.commit();
	}

	private Indexing update(Subject subject) {
		return new Indexing() {
			private final int id = subjects.id(subject);
			private final List<Integer> currentTerms = registry.fullTermsOf(id);
			private final List<Integer> exclusiveTerms = registry.selfTermsOf(id);

			@Override
			public Indexing set(Term term) {
				int[] candidateTerms = termsWith(term.tag());
				return candidateTerms.length == 1 ?
						exclusiveTerms.contains(candidateTerms[0]) ?
							modifyTerm(term, candidateTerms[0]) :
							replaceTerm(term, candidateTerms[0]) :
						replaceTerms(term, candidateTerms);
			}

			private Indexing modifyTerm(Term term, int id) {
				registry.setTerm(id, term.toString());
				terms.set(id, term);
				return this;
			}

			private Indexing replaceTerm(Term term, int id) {
				registry.unlink(this.id, id);
				return link(term);
			}

			private Indexing replaceTerms(Term term, int[] candidateTerms) {
				del(candidateTerms);
				return link(term);
			}

			public Indexing put(Term term) {
				return link(term);
			}

			private Indexing link(Term term) {
				int id = terms.add(term);
				registry.link(this.id, id);
				return this;
			}

			@Override
			public Indexing del(String tag) {
				int[] candidateTerms = termsWith(tag);
				return del(candidateTerms);
			}

			@Override
			public Indexing del(Term term) {
				return del(id(term));
			}

			private Indexing del(int id) {
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

			private Indexing del(int[] terms) {
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
		subject.children().collect().forEach(this::drop);
	}

	private void dropIndexOf(Subject subject) {
		int id = subjects.id(subject);
		terms.remove(registry.selfTermsOf(id));
		subjects.remove(subject);
		registry.deleteSubject(id);
	}

	@SuppressWarnings("resource")
	private void dropHistoryOf(Subject subject) {
		if (subject.hasHistory()) subject.history().drop();
	}

	public Statements statements() {
		return this::stamentIterator;
	}

	public SubjectIndex consume(Statements statements) {
		Batch batch = batch();
		for (Triple triple : statements)
			batch.put(triple);
		batch.terminate();
		return this;
	}

	public void dump(OutputStream os) throws IOException {
		for (Triple triple : statements()) {
			String str = triple.toString() + '\n';
			os.write(str.getBytes());
		}
	}

	public SubjectIndex restore(InputStream is) throws IOException {
		try (DumpStatements statements = new DumpStatements(is)) {
			return consume(statements);
		}
	}

	private Iterator<Triple> stamentIterator() {
		return registry.dump()
				.map(Triple::new)
				.iterator();
	}

	private Context createContext() {
		return new Context() {

			@Override
			public List<Subject> children(Subject subject) {
				return subjects.stream()
						.filter(s -> s.parent().equals(subject))
						.map(s -> wrap(s))
						.toList();
			}

			@Override
			public List<Term> terms(Subject subject) {
				int id = subjects.id(subject);
				return id < 0 ? List.of() : toTerms(registry.fullTermsOf(id));
			}

			private List<Term> toTerms(List<Integer> terms) {
				return terms.stream()
						.map(SubjectIndex.this.terms::get)
						.toList();
			}

			@Override
			public Subject create(Subject subject) {
				return SubjectIndex.this.create(subject.identifier());
			}

			@Override
			public Subject get(String identifier) {
				return SubjectIndex.this.open(identifier);
			}

			@Override
			public void rename(Subject subject, String identifier) {
				SubjectIndex.this.rename(subject, identifier);
			}

			@Override
			public Indexing index(Subject subject) {
				return SubjectIndex.this.update(subject);
			}

			@Override
			public void drop(Subject subject) {
				SubjectIndex.this.drop(subject);
			}

			@Override
			public SubjectHistory history(Subject subject) {
				return new SubjectHistory(subject.identifier(), SqlConnection.shared(jdbcUrl));
			}

			@Override
			public boolean hasHistory(Subject subject) {
				return SubjectIndex.this.hasHistory(subject);
			}
		};
	}

	private boolean hasHistory(Subject subject) {
		return registry.hasHistory(subjects.id(subject));
	}

	private Stream<Integer> subjectsWith(Set<String> types) {
		return subjectsWith(types.isEmpty() ? s->true : predicateFor(types));
	}

	private Stream<Integer> subjectsWith(Predicate<Subject> predicate) {
		return subjects.stream()
				.filter(predicate)
				.map(subjects::id);
	}

	private Predicate<Subject> predicateFor(Set<String> types) {
		return types.contains(Any) ? s->true : s -> types.contains(s.type());
	}

	private Subject wrap(Subject subject) {
		return new Subject(subject, context);
	}

	private int insert(Subject subject) {
		int index = subjects.indexOfNull();
		if (index <= 0) return registry.insertSubject(subject.identifier());
		registry.setSubject(index, subject.identifier());
		return index;
	}

	private int insert(Term term) {
		int index = terms.indexOfNull();
		if (index <= 0) return registry.insertTerm(term.toString());
		registry.setTerm(index, term.toString());
		return index;
	}

	private List<Subject> toSubjects(List<Integer> subjects) {
		return subjects.stream().map(this::toSubject).toList();
	}

	private Subject toSubject(int subject) {
		return wrap(this.subjects.get(subject));
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

		default void put(Triple triple) {
			put(triple.subject(), triple.term());
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
