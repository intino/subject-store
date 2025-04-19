package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Subject;

public class SubjectStore implements AutoCloseable {
	private final String storage;
	private final SubjectIndex index;

	public SubjectStore(String storage) {
		this.storage = storage;
		this.index = new SubjectIndex(storage);
	}

	public boolean has(String identifier) {
		return index.has(identifier);
	}

	public boolean has(String name, String type) {
		return index.has(name, type);
	}

	public Subject get(String identifier) {
		return index.get(identifier);
	}

	public Subject get(String name, String type) {
		return index.get(name, type);
	}

	public Subject create(String identifier) {
		return index.create(identifier);
	}

	public Subject create(String name, String type) {
		return index.create(name, type);
	}

	public SubjectQuery subjects() {
		return index.query();
	}

	public SubjectQuery subjects(String... types) {
		return index.query(types);
	}

	@Override
	public void close()  {
		try {
			index.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public SubjectIndexView.Builder view() {
		return SubjectIndexView.of(index);
	}

	public SubjectHistoryView.Builder view(Subject subject) {
		return SubjectHistoryView.of(subject.history());
	}

	public SubjectHistoryView.Builder viewOf(String identifier) {
		return view(get(identifier));
	}

	public SubjectHistoryView.Builder view(String name, String type) {
		return view(get(name, type));
	}


}
