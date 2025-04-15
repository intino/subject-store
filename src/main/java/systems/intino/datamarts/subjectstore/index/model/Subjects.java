package systems.intino.datamarts.subjectstore.index.model;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record Subjects(List<Subject> items) implements Iterable<Subject> {

	public boolean isEmpty() {
		return items.isEmpty();
	}

	public int size() {
		return items.size();
	}

	public Subject get(int index) {
		return items.get(index);
	}

	public Subjects filter(Predicate<Subject> predicate) {
		return new Subjects(items.stream().filter(predicate).toList());
	}

	public Stream<Subject> stream() {
		return items.stream();
	}

	@Override
	public Iterator<Subject> iterator() {
		return items.iterator();
	}

	public String serialize() {
		return items.stream()
				.filter(Objects::nonNull)
				.map(Subject::toString)
				.collect(Collectors.joining("\n"));
	}
}
