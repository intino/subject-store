package systems.intino.datamarts.subjectstore.pools;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import systems.intino.datamarts.subjectstore.model.Term;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class TermPool {
	private final List<String> terms;
	private final Cache<String, Integer> cache;

	public TermPool() {
		this.terms = new ArrayList<>();
		this.cache = Caffeine.newBuilder()
				.maximumSize(500_000)
				.build();
	}

	public boolean contains(int id) {
		return id >= 0 && id < terms.size() && term(id) != null;
	}

	public int id(Term term) {
		return id(term.toString());
	}

	private int id(String term) {
		Integer id = cache.getIfPresent(term);
		return id != null ? id : -1;
	}

	private int indexOf(String term) {
		return terms.indexOf(term);
	}

	public Term term(int id) {
		String value = terms.get(id);
		return value != null ? Term.of(value) : null;
	}

	public List<Term> terms(List<Integer> ids) {
		return ids.stream().map(this::term).toList();
	}

	public int add(Term term) {
		return add(term.toString());
	}

	public int add(String term) {
		int id = id(term);
		return id >= 0 ? id : put(term);
	}

	public int put(String term) {
		int id = terms.size();
		cache.put(term, id);
		terms.add(term);
		return id;
	}

	public void remove(Term term) {
		remove(term.toString());
	}

	private void remove(String term) {
		terms.set(id(term), null);
		cache.invalidate(term);
	}

	public void remove(int id) {
		remove(term(id));
	}

	public List<Integer> pull(String tag) {
		return IntStream.range(0, terms.size())
				.filter(i -> terms.get(i) != null)
				.filter(i->Term.of(terms.get(i)).is(tag))
				.peek(i->cache.put(terms.get(i), i))
				.boxed()
				.toList();
	}
}
