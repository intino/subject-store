package systems.intino.datamarts.subjectstore.pools;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.util.*;
import java.util.stream.Stream;

public class SubjectPool {
	private final List<String> subjects;
	private final Cache<String, Integer> cache;

	public SubjectPool() {
		this.subjects = new ArrayList<>();
		this.cache = Caffeine.newBuilder()
				.maximumSize(10_000)
				.build();
	}

	public int size() {
		return subjects.size();
	}

	public boolean contains(Subject subject) {
		return id(subject) >= 0;
	}

	public boolean contains(int id) {
		return id >= 0 && id < subjects.size() && subject(id) != null;
	}

	public int id(Subject subject) {
		return id(subject.identifier());
	}

	private int id(String subject) {
		return cache.get(subject, this::indexOf);
	}

	private int indexOf(String subject) {
		int index = subjects.indexOf(subject);
		return (index >= 0) ? index : -1;
	}

	public Subject subject(int id) {
		String identifier = subjects.get(id);
		return identifier != null ? new Subject(identifier) : null;
	}

	public int add(Subject subject) {
		return add(subject.identifier());
	}

	private int add(String subject) {
		int id = id(subject);
		if (id >= 0) return id;
		cache.put(subject, put(subject));
		return subjects.size()-1;
	}

	public int put(String subject) {
		subjects.add(subject);
		return subjects.size()-1;
	}

	public void remove(Subject subject) {
		int id = id(subject);
		if (id < 0) return;
		fix(id, (String) null);
	}

	public void fix(int id, Subject subject) {
		fix(id, subject.identifier());
	}

	private void fix(int id, String subject) {
		cache.invalidate(subjects.get(id));
		subjects.set(id, subject);
	}

	public Stream<Subject> stream() {
		return subjects.stream()
				.filter(Objects::nonNull)
				.map(Subject::new);
	}
}
