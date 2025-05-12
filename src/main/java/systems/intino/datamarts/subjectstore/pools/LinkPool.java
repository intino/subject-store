package systems.intino.datamarts.subjectstore.pools;

import systems.intino.datamarts.subjectstore.helpers.Triskel;

import java.util.Iterator;
import java.util.List;

public class LinkPool {
	private final Triskel triskel;

	public LinkPool() {
		this.triskel = new Triskel();
	}

	public boolean exists(int subject, int term) {
		return subject >= 0 && term >= 0 && triskel.get(subject, term);
	}

	public LinkPool add(int subject, int term) {
		triskel.set(subject, term);
		return this;
	}

	public List<Integer> remove(int subject) {
		List<Integer> ids = termsOf(subject);
		ids.forEach(term -> triskel.unset(subject, term));
		return ids;
	}

	public List<Integer> termsOf(int subject) {
		return triskel.horizontalItemsIn(subject);
	}

	public void remove(int subject, int term) {
		if (!triskel.get(subject, term)) return;
		triskel.unset(subject, term);
	}

	public boolean termIsUsed(int term) {
		return triskel.isColActive(term);
	}

	public List<Integer> subjectsWith(int term) {
		return triskel.verticalItemsIn(term);
	}

	public Iterator<int[]> iterator() {
		return triskel.links().iterator();
	}
}
