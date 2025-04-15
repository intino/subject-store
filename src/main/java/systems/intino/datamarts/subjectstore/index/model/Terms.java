package systems.intino.datamarts.subjectstore.index.model;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record Terms(List<Term> items) implements Iterable<Term> {
	public boolean isEmpty() {
		return items.isEmpty();
	}

	public int size() {
		return items.size();
	}

	public Term get(int index) {
		return items.get(index);
	}

	public Values get(String name) {
		return get(values(t->t.tag().equals(name)));
	}

	private Values get(List<String> values) {
		return new Values() {
			@Override
			public String first() {
				return values.getFirst();
			}

			@Override
			public String serialize() {
				return values.stream().map(Object::toString).collect(Collectors.joining("\n"));
			}

			@Override
			public Iterator<String> iterator() {
				return values.iterator();
			}
		};
	}

	private List<String> values(Predicate<Term> predicate) {
		return items.stream().filter(predicate).map(Term::value).toList();
	}

	public Terms filter(Predicate<Term> predicate) {
		return new Terms(items.stream().filter(predicate).collect(Collectors.toList()));
	}

	@Override
	public Iterator<Term> iterator() {
		return items.iterator();
	}

	public Stream<Term> stream() {
		return items.stream();
	}

	public String serialize() {
		return items.stream()
				.map(Term::toString)
				.collect(Collectors.joining("\n"));
	}

	public interface Values extends Iterable<String> {
		String first();

		String serialize();
	}
}
