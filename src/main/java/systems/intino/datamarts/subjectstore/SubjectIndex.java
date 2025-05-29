package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.helpers.SubjectQueryParser;
import systems.intino.datamarts.subjectstore.model.Triples;
import systems.intino.datamarts.subjectstore.io.triples.DumpTriples;
import systems.intino.datamarts.subjectstore.model.*;
import systems.intino.datamarts.subjectstore.model.Subject.Context;
import systems.intino.datamarts.subjectstore.model.Subject.Updating;
import systems.intino.datamarts.subjectstore.model.journals.FileJournal;
import systems.intino.datamarts.subjectstore.pools.LinkPool;
import systems.intino.datamarts.subjectstore.pools.StringPool;

import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectstore.model.Journal.Transaction.Type.*;

public class SubjectIndex {
	private final FileJournal journal;
	private final StringPool subjectPool;
	private final StringPool termPool;
	private final LinkPool linkPool;
	private final Context context;

	public SubjectIndex(File journal) {
		this.journal = new FileJournal(journal);
		this.subjectPool = new StringPool();
		this.termPool = new StringPool();
		this.linkPool = new LinkPool();
		this.context = createContext();
	}

	public boolean has(String name, String type) {
		return has(Subject.of(name, type).identifier());
	}

	public boolean has(String identifier) {
		return subjectPool.contains(identifier);
	}

	private boolean has(int identifier) {
		return subjectPool.contains(identifier);
	}

	public Subject open(String name, String type) {
		return open(Subject.of(name, type).identifier()) ;
	}

	public Subject open(String identifier) {
		return has(identifier) ? new Subject(identifier, context) : null;
	}

	private Subject open(int id) {
		String value = subjectPool.get(id);
		return value != null ? Subject.of(value, context) : null;
	}

	private Term term(int id) {
		String value = termPool.get(id);
		return value != null ? Term.of(value) : Term.Null;
	}

	public SubjectQuery subjects(String query) {
		return new SubjectQueryParser(this).parse(query);
	}

	public SubjectQuery subjects() {
		return new SubjectQuery() {
			private final SubjectQuery This = this;
			private final Sorting sorting = new Sorting();
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
				return candidates()
						.mapToObj(i-> open(i))
						.sorted(sorting::sort);
			}

			@Override
			public List<Subject> collect() {
				return stream().toList();
			}

			@Override
			public SubjectQuery isType(String type) {
				conditions.add(id-> is(type, id));
				return this;
			}

			@Override
			public SubjectQuery isRoot() {
				conditions.add(this::isRoot);
				return this;
			}

			@Override
			public SubjectQuery isChildOf(String identifier) {
				conditions.add(id -> isChildOf(id, identifier));
				return this;
			}

			@Override
			public SubjectQuery isUnderOf(String identifier) {
				conditions.add(id -> isUnderOf(id, identifier));
				return this;
			}

			private boolean isRoot(int id) {
				Subject subject = open(id);
				return subject != null && subject.isRoot();
			}

			private boolean isChildOf(Integer id, String identifier) {
				Subject subject = open(id);
				return subject != null && subject.isChildOf(identifier);
			}

			private boolean isUnderOf(Integer id, String identifier) {
				Subject subject = open(id);
				return subject != null && subject.isUnderOf(identifier);
			}

			private boolean is(String type, int id) {
				Subject subject = open(id);
				return subject != null && subject.is(type);
			}

			@Override
			public SubjectQuery orderBy(String tag, Comparator<String> comparator) {
				sorting.add(tag, comparator);
				return this;
			}

			@Override
			public AttributeFilter where(String tag) {
				return where(termHas(tag));
			}

			private List<Integer> termHas(String tag) {
				return termPool.ids(s -> Term.of(s).is(tag));
			}

			private AttributeFilter where(List<Integer> terms) {
				return new AttributeFilter() {

					@Override
					public SubjectQuery satisfy(Predicate<String> predicate) {
						List<Integer> selection = termsThat(predicate);
						if (selection.isEmpty()) return SubjectQuery.Empty;
						Set<Integer> subjects = subjectWith(selection);
						conditions.add(subjects::contains);
						return This;
					}

					private List<Integer> termsThat(Predicate<String> predicate) {
						List<Integer> result = new ArrayList<>();
						for (int term : terms)
							if (predicate.test(term(term).value()))
								result.add(term);
						return result;
					}

					private Set<Integer> subjectWith(List<Integer> terms) {
						Set<Integer> subjects = new HashSet<>();
						for (int term : terms)
							subjects.addAll(linkPool.subjectsWith(term));
						return subjects;
					}

				};
			}

			private IntStream candidates() {
				return subjectsWith(conditions());
			}

			private IntStream subjectsWith(Predicate<Integer> conditions) {
				return IntStream.range(0, subjectPool.size())
						.filter(s -> subjectPool.get(s) != null)
						.filter(conditions::test);
			}

			private Predicate<Integer> conditions() {
				return conditions.stream().reduce(x -> true, Predicate::and);
			}

		};
	}

	public Subject create(String name, String type) {
		return create(Subject.of(name, type));
	}

	public Subject create(String identifier) {
		return create(Subject.of(identifier));
	}

	public Subject create(Subject subject) {
		synchronized (journal) {
			int id = subjectPool.add(subject.identifier());
			return open(id);
		}
	}

	private void rename(Subject subject, String identifier) {
		int id = subjectPool.id(subject.identifier());
		subjectPool.fix(id, identifier);
	}

	private void drop(Subject subject) {
		if (!has(subject.identifier())) return;
		dropChildrenOf(subject);
		dropTermsOf(subject);
		subjectPool.remove(subject.identifier());
	}

	private Updating update(Subject subject) {
		return new Updating() {
			private final int id = subjectPool.id(subject.identifier());

			@Override
			public Updating put(Term term) {
				synchronized (journal) {
					if (term.isEmpty() || existsLink(term)) return this;
					journal.add(new Journal.Transaction(put, subject.identifier(), term.toString()));
					return write(term);
				}
			}

			@Override
			public Updating set(Term term) {
				if (term.value().isEmpty()) return del(term.tag());
				synchronized (journal) {
					journal.add(new Journal.Transaction(set, subject.identifier(), term.toString()));
					termsWith(term.tag()).forEach(this::erase);
					return write(term);
				}
			}

			@Override
			public Updating del(Term term) {
				synchronized (journal) {
					if (!existsTerm(term)) return this;
					journal.add(new Journal.Transaction(del, subject.identifier(), term.toString()));
					return erase(term);
				}
			}

			@Override
			public Updating del(String tag) {
				termsWith(tag).forEach(this::del);
				return this;
			}

			private boolean existsTerm(Term term) {
				return termPool.id(term.serialize()) >= 0;
			}

			private boolean existsLink(Term term) {
				int id = termPool.id(term.serialize());
				return id >= 0 && linkPool.exists(this.id, id);
			}

			private Updating write(Term term) {
				linkPool.add(id, termPool.add(term.serialize()));
				return this;
			}

			private Updating erase(Term term) {
				int id = termPool.id(term.serialize());
				linkPool.remove(this.id, id);
				if (!linkPool.termIsUsed(id)) termPool.remove(term.serialize());
				return this;
			}

			private Stream<Term> termsWith(String tag) {
				return subject.terms().stream().filter(t->t.is(tag));
			}

		};
	}

	private void dropChildrenOf(Subject subject) {
		subject.children().collect().forEach(this::drop);
	}

	private void dropTermsOf(Subject subject) {
		List<Integer> usedTerms = linkPool.remove(subjectPool.id(subject.identifier()));
		usedTerms.stream()
				.filter(term -> linkPool.subjectsWith(term).isEmpty())
				.forEach(termPool::remove);
	}

	public Triples triples() {
		return this::tripleIterator;
	}

	public void dump(OutputStream os) throws IOException {
		for (Triple triple : triples()) {
			String str = triple.toString() + '\n';
			os.write(str.getBytes());
		}
	}

	public SubjectIndex restore(InputStream is) throws IOException {
		try (DumpTriples triples = new DumpTriples(is)) {
			return restore(triples);
		}
	}

	public SubjectIndex restore(Triples triples) {
		Batch batch = batch();
		for (Triple triple : triples) batch.put(triple);
		System.gc();
		return this;
	}

	public SubjectIndex restore(Journal journal) {
		if (journal.isEmpty()) return this;
		for (Journal.Transaction transaction : journal.transactions()) {
			Subject subject = create(transaction.subject());
			switch (transaction.type()) {
				case put -> subject.update().put(Term.of(transaction.parameter()));
				case set -> subject.update().set(Term.of(transaction.parameter()));
				case del -> subject.update().del(Term.of(transaction.parameter()));
				case drop -> subject.drop();
				case rename -> subject.rename(transaction.parameter());
			}
		}
		return this;
	}

	private Iterator<Triple> tripleIterator() {
		return new Iterator<>() {
			final Iterator<int[]> iterator = linkPool.iterator();

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Triple next() {
				int[] id = iterator.next();
				String identifier = subjectPool.get(id[0]);
				Term term = term(id[1]);
				return new Triple(identifier, term.tag(), term.value());
			}
		};
	}

	private Context createContext() {
		return new Context() {

			@Override
			public List<Subject> children(Subject subject) {
				return subjectPool.stream()
						.filter(s -> s.startsWith(subject.identifier()))
						.map(s -> open(s))
						.filter(s -> s.parent().equals(subject))
						.toList();
			}

			@Override
			public List<Term> terms(Subject subject) {
				int id = subjectPool.id(subject.identifier());
				return id < 0 ? List.of() : linkPool.termsOf(id).stream().map(i->term(i)).toList();
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
			public Updating update(Subject subject) {
				return SubjectIndex.this.update(subject);
			}

			@Override
			public void rename(Subject subject, String identifier) {
				synchronized (journal) {
					journal.add(new Journal.Transaction(rename, subject.identifier(), nameIn(identifier)));
					SubjectIndex.this.rename(subject, identifier);
				}
			}

			@Override
			public void drop(Subject subject) {
				synchronized (journal) {
					journal.add(new Journal.Transaction(drop, subject.identifier(), "-"));
					SubjectIndex.this.drop(subject);
				}
			}
		};
	}

	private String nameIn(String identifier) {
		return Subject.of(identifier).name();
	}

	public Batch batch() {
		return new Batch() {
			private String last;
			private int id;
			@Override
			public void put(Triple triple) {
				linkPool.add(id(triple.subject()), termPool.add(triple.term()));
			}

			private int id(String subject) {
				if (subject.equals(last)) return id;
				this.last = subject;
				id = subjectPool.create(subject);
				return id;
			}
		};
	}

	public Context context() {
		return context;
	}

	public interface Batch {
		void put(Triple triple);
	}



}
