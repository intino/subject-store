package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Subject;

public class SubjectStore implements AutoCloseable {
	private final SubjectIndex index;

	public SubjectStore(String storage) {
		this.index = new SubjectIndex(storage);
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
		return index.subjects();
	}

	public SubjectQuery subjects(String... types) {
		return index.subjects(types);
	}

	@Override
	public void close()  {
		try {
			index.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
