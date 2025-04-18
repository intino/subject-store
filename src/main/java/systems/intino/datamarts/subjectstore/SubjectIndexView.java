package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Statement;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.index.view.Column;
import systems.intino.datamarts.subjectstore.model.Term;

import java.util.*;
import java.util.stream.Stream;

public class SubjectIndexView implements Iterable<Column>  {
	private final List<Subject> subjects;
	private final List<String> keys;

	public static Builder of(SubjectIndex subjectIndex) {
		return new Builder(subjectIndex);
	}

	private SubjectIndexView(List<Subject> subjects, List<String> keys) {
		this.subjects = subjects;
		this.keys = keys;
	}

	public Statement[] get(int index) {
		return statements(subjects.get(index));
	}

	public Statement[] get(int index, String key) {
		Subject subject = subjects.get(index);
		return statements(subject, key).toArray(Statement[]::new);
	}

	public int size() {
		return subjects.size();
	}

	public int columns() {
		return keys.size();
	}

	public Column column(String name) {
		return new Column(name, valuesOf(name));
	}

	private String[] valuesOf(String key) {
		return subjects.stream()
				.flatMap(s-> statements(s, key))
				.map(s->s.term().value())
				.toArray(String[]::new);
	}

	@Override
	public Iterator<Column> iterator() {
		return new Iterator<>() {
			private final Iterator<String> iterator = keys.iterator();

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Column next() {
				return column(iterator.next());
			}
		};
	}

	private Statement[] statements(Subject subject) {
		return keys.stream()
				.flatMap(s -> statements(subject, s))
				.toArray(Statement[]::new);
	}

	private Stream<Statement> statements(Subject subject, String key) {
		return termsOf(subject).stream()
				.filter(t->t.is(key))
				.map(t->new Statement(subject, t));
	}

	private final Map<Subject, List<Term>> terms = new HashMap<>();
	private List<Term> termsOf(Subject subject) {
		return terms.computeIfAbsent(subject, Subject::terms);
	}

	public static class Builder {
		private final SubjectIndex subjectIndex;
		private final List<String> types;
		private final List<String> keys;

		public Builder(SubjectIndex subjectIndex) {
			this.subjectIndex = subjectIndex;
			this.types = new ArrayList<>();
			this.keys = new ArrayList<>();
		}

		public Builder type(String type) {
			types.add(type);
			return this;
		}

		public Builder add(String key) {
			keys.add(key);
			return this;
		}

		public SubjectIndexView build() {
			List<Subject> subjects = subjectIndex.query(types).collect();
			return new SubjectIndexView(subjects, keys);
		}

	}

}
