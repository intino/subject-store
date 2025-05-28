package systems.intino.datamarts.subjectstore.pools;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.*;
import java.util.stream.Stream;

public class SubjectPool {
	private final List<String> values;
	private final Cache<String, Integer> cache;

	public SubjectPool() {
		this.values = new ArrayList<>();
		this.cache = Caffeine.newBuilder()
				.maximumSize(10_000)
				.build();
	}

	public int size() {
		return values.size();
	}

	public boolean contains(String value) {
		return id(value) >= 0;
	}

	public boolean contains(int id) {
		return id >= 0 && id < values.size() && get(id) != null;
	}

	public String get(int id) {
		return values.get(id);
	}

	public int id(String value) {
		return cache.get(value, this::indexOf);
	}

	private int indexOf(String value) {
		int index = values.indexOf(value);
		return (index >= 0) ? index : -1;
	}

	public int add(String value) {
		int id = id(value);
		if (id >= 0) return id;
		cache.put(value, create(value));
		return values.size() - 1;
	}

	public int create(String value) {
		values.add(value);
		return values.size() - 1;
	}

	public void remove(String value) {
		int id = id(value);
		if (id < 0) return;
		fix(id, null);
	}

	public void fix(int id, String value) {
		cache.invalidate(values.get(id));
		values.set(id, value);
	}

	public Stream<String> stream() {
		return values.stream()
				.filter(Objects::nonNull);
	}
}
