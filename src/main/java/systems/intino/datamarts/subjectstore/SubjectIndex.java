package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.SubjectIndex.Journal.Transaction;
import systems.intino.datamarts.subjectstore.model.Triples;
import systems.intino.datamarts.subjectstore.io.triples.DumpTriples;
import systems.intino.datamarts.subjectstore.model.*;
import systems.intino.datamarts.subjectstore.model.Subject.Context;
import systems.intino.datamarts.subjectstore.model.Subject.Updating;
import systems.intino.datamarts.subjectstore.pools.LinkPool;
import systems.intino.datamarts.subjectstore.pools.SubjectPool;
import systems.intino.datamarts.subjectstore.pools.TermPool;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static systems.intino.datamarts.subjectstore.SubjectIndex.Journal.Type.*;

public class SubjectIndex {
	private final Journal journal;
	private final SubjectPool subjectPool;
	private final LinkPool linkPool;
	private final TermPool termPool;

	public SubjectIndex(File journal) {
		this.journal = new Journal(journal);
		this.subjectPool = new SubjectPool();
		this.linkPool = new LinkPool();
		this.termPool = new TermPool();
		Subject.context = createContext();
	}

	public boolean has(String name, String type) {
		return subjectPool.contains(new Subject(name, type));
	}

	public boolean has(String identifier) {
		return subjectPool.contains(new Subject(identifier));
	}

	public Subject open(String name, String type) {
		Subject subject = new Subject(name, type);
		return subjectPool.contains(subject) ? subject : null;
	}

	public Subject open(String identifier) {
		Subject subject = Subject.of(identifier);
		return subjectPool.contains(subject) ? subject : null;
	}

	private Subject open(int subject) {
		return subjectPool.contains(subject) ? subjectPool.subject(subject) : null;
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
						.mapToObj(i->toSubject(i))
						.sorted(sorting::sort);
			}

			@Override
			public List<Subject> collect() {
				return stream().toList();
			}

			@Override
			public SubjectQuery type(String type) {
				conditions.add(s->subject(s).is(type));
				return this;
			}

			@Override
			public SubjectQuery isRoot() {
				conditions.add(s->subject(s).isRoot());
				return this;
			}

			@Override
			public SubjectQuery orderBy(String tag, Comparator<String> comparator) {
				sorting.add(tag, comparator);
				return this;
			}

			@Override
			public AttributeFilter where(String tag) {
				return where(termPool.pull(tag));
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

					private Term term(int id) {
						return termPool.term(id);
					}
				};
			}

			private IntStream candidates() {
				return subjectsWith(conditions());
			}

			private IntStream subjectsWith(Predicate<Integer> conditions) {
				return IntStream.range(0, subjectPool.size())
						.filter(s -> subjectPool.subject(s) != null)
						.filter(conditions::test);
			}

			private Predicate<Integer> conditions() {
				return conditions.stream().reduce(x -> true, Predicate::and);
			}

			private Subject subject(int id) {
				return subjectPool.subject(id);
			}

		};
	}

	public Subject create(Subject subject) {
		synchronized (journal) {
			int id = subjectPool.add(subject);
			return open(id);
		}
	}

	public Subject create(String name, String type) {
		return create(new Subject(name, type));
	}

	public Subject create(String identifier) {
		return create(Subject.of(identifier));
	}

	private void rename(Subject subject, String identifier) {
		int id = subjectPool.id(subject);
		subjectPool.fix(id, new Subject(identifier));
	}

	private void drop(Subject subject) {
		if (!exists(subject)) return;
		dropChildrenOf(subject);
		dropTermsOf(subject);
		subjectPool.remove(subject);
	}

	private Updating update(Subject subject) {
		return new Updating() {
			private final int id = subjectPool.id(subject);

			@Override
			public Updating put(Term term) {
				synchronized (journal) {
					if (term.isEmpty() || existsLink(term)) return this;
					journal.add(new Transaction(put, subject, term.toString()));
					return write(term);
				}
			}

			@Override
			public Updating set(Term term) {
				synchronized (journal) {
					journal.add(new Transaction(set, subject, term.toString()));
					termsWith(term.tag()).forEach(this::erase);
					return write(term);
				}
			}

			@Override
			public Updating del(Term term) {
				synchronized (journal) {
					if (!existsTerm(term)) return this;
					journal.add(new Transaction(del, subject, term.toString()));
					return erase(term);
				}
			}

			@Override
			public Updating del(String tag) {
				termsWith(tag).forEach(this::del);
				return this;
			}

			private boolean existsTerm(Term term) {
				return termPool.id(term) >= 0;
			}

			private boolean existsLink(Term term) {
				int id = termPool.id(term);
				return id >= 0 && linkPool.exists(this.id, id);
			}

			private Updating write(Term term) {
				linkPool.add(id, termPool.add(term));
				return this;
			}

			private Updating erase(Term term) {
				int id = termPool.id(term);
				linkPool.remove(this.id, id);
				if (!linkPool.termIsUsed(id)) termPool.remove(term);
				return this;
			}

			private Stream<Term> termsWith(String tag) {
				return subject.terms().stream().filter(t->t.is(tag));
			}

		};
	}

	private boolean exists(Subject subject) {
		return !subject.isNull() && subjectPool.contains(subject);
	}

	private void dropChildrenOf(Subject subject) {
		subject.children().collect().forEach(this::drop);
	}

	private void dropTermsOf(Subject subject) {
		List<Integer> usedTerms = linkPool.remove(subjectPool.id(subject));
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
		for (Transaction transaction : journal.statements()) {
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
				Subject subject = subjectPool.subject(id[0]);
				Term term = termPool.term(id[1]);
				return new Triple(subject.identifier(), term.tag(), term.value());
			}
		};
	}

	private Context createContext() {
		return new Context() {


			@Override
			public List<Subject> children(Subject subject) {
				return subjectPool.stream()
						.filter(s -> s.parent().equals(subject))
						.toList();
			}

			@Override
			public List<Term> terms(Subject subject) {
				int id = subjectPool.id(subject);
				return id < 0 ? List.of() : termPool.terms(linkPool.termsOf(id));
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
					journal.add(new Transaction(rename, subject, nameIn(identifier)));
					SubjectIndex.this.rename(subject, identifier);
				}
			}

			@Override
			public void drop(Subject subject) {
				synchronized (journal) {
					journal.add(new Transaction(drop, subject, "-"));
					SubjectIndex.this.drop(subject);
				}
			}
		};
	}

	private String nameIn(String identifier) {
		return new Subject(identifier).name();
	}

	private Subject toSubject(int subject) {
		return subjectPool.subject(subject);
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


	public interface Batch {
		void put(Triple triple);
	}

	public static class Journal {
		private final Path path;

		public Journal(File file) {
			this.path = file.toPath();
		}

		public List<Transaction> statements() {
			return linesIn().stream()
					.map(this::transaction)
					.toList();
		}

		private List<String> linesIn() {
			try {
				return Files.readAllLines(path);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private Transaction transaction(String s) {
			String[] split = s.split(" ", 3);
			return new Transaction(valueOf(split[0]), new Subject(split[1]), split[2]);
		}


		public void add(Transaction transaction) {
			try {
				Files.write(path, (transaction.toString() + "\n").getBytes(), CREATE, APPEND);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public boolean isEmpty() {
			return !path.toFile().exists();
		}

		public record Transaction(Type type, Subject subject, String parameter) {
			@Override
			public String toString() {
				return type + " " + subject + " " + parameter;
			}
		}

		public enum Type {
			put, set, del, drop, rename
		}
	}
}
