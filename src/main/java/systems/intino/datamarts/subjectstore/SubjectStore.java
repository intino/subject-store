package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Subject;

import java.util.List;

public class SubjectStore implements AutoCloseable {
	private final SubjectIndex index;

	public SubjectStore(String jdbcUrl) {
		this.index = new SubjectIndex(jdbcUrl);
	}

	public boolean has(String identifier) {
		return index.has(identifier);
	}

	public boolean has(String name, String type) {
		return index.has(name, type);
	}

	public Subject open(String identifier) {
		return index.open(identifier);
	}

	public Subject open(String name, String type) {
		return index.open(name, type);
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

	@Override
	public void close()  {
		try {
			index.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


	public SubjectIndexView.Builder viewOf(List<Subject> subjects) {
		return SubjectIndexView.of(subjects);
	}

	public SubjectHistoryView.Builder viewOf(Subject subject) {
		return SubjectHistoryView.of(subject.history());
	}

	public SubjectHistoryView.Builder viewOf(String identifier) {
		return viewOf(open(identifier));
	}

	public SubjectHistoryView.Builder viewOf(String name, String type) {
		return viewOf(open(name, type));
	}


}
