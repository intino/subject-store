package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Triple;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.model.Term;
import systems.intino.datamarts.subjectstore.view.index.Column;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class SubjectIndexView implements Iterable<Column>  {
	private final SubjectIndex index;
	private final List<Column> columns;
	private final List<Sort> sorts;
	private final List<Subject> subjects = new ArrayList<>();

	public static Builder of(SubjectIndex index) {
		return new Builder(index);
	}

	private SubjectIndexView(SubjectIndex index, List<Column> columns, List<Sort> sorts) {
		this.index = index;
		this.columns = columns;
		this.sorts = sorts;
	}

	public static Builder of(List<Subject> subjects) {
		return new Builder(null);
	}

	public Triple[] get(int index) {
		return statements(subjects.get(index));
	}

	public Triple[] get(int index, String column) {
		Subject subject = subjects.get(index);
		return statements(subject, column).toArray(Triple[]::new);
	}

	public int size() {
		return subjects.size();
	}

	public List<String> rows() {
		return subjects.stream().map(Subject::identifier).toList();
	}

	public List<Column> columns() {
		return columns;
	}

	public Column column(String name) {
		return columns.stream().filter(c->c.name().equals(name)).findFirst().orElse(null);
	}

	private String[] valuesOf(String key) {
		return subjects.stream()
				.flatMap(s-> statements(s, key))
				.map(s->s.term().value())
				.toArray(String[]::new);
	}

	@Override
	public Iterator<Column> iterator() {
		return columns.iterator();
	}

	public void exportTo(OutputStream os) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
			writer.write(tsv());
		}
	}

	private String tsv() {
		StringBuilder sb = new StringBuilder();
		for (Subject subject : subjects) {
			StringJoiner line = new StringJoiner("\t");
			for (Column column : columns())
				line.add(value(subject, column.name()));
			sb.append(line).append('\n');
		}
		return sb.toString();
	}

	private Triple[] statements(Subject subject) {
		return columns.stream()
				.flatMap(s -> statements(subject, s.name()))
				.toArray(Triple[]::new);
	}

	private Stream<Triple> statements(Subject subject, String tag) {
		return termsOf(subject).stream()
				.filter(term->term.is(tag))
				.map(term->new Triple(subject, term));
	}

	private String value(Subject subject, String tag) {
		return termsOf(subject).stream()
				.filter(term->term.is(tag))
				.map(Term::value)
				.collect(joining(", "));
	}

	private final Map<Subject, List<Term>> terms = new HashMap<>();
	private List<Term> termsOf(Subject subject) {
		return terms.computeIfAbsent(subject, Subject::terms);
	}

	public static class Builder {
		private final SubjectIndex index;
		private final List<String> types;
		private final List<Column> columns;
		private final List<Sort> sorts;

		public Builder(SubjectIndex index) {
			this.index = index;
			this.types = new ArrayList<>();
			this.columns = new ArrayList<>();
			this.sorts = new ArrayList<>();
		}

		public Builder type(String type) {
			types.add(type);
			return this;
		}

		public Builder add(String tag, Column.Type type) {
			columns.add(new Column() {
				@Override
				public String name() {
					return tag;
				}

				@Override
				public Type type() {
					return type;
				}
			});
			return this;
		}

		public Builder sort(String tag, SortDirection direction) {
			sorts.add(new Sort(tag, direction));
			return this;
		}

		public SubjectIndexView build() {
			return new SubjectIndexView(index, columns, sorts);
		}
	}

	public enum SortDirection {
		Ascending, Descending
	}

	public record Sort(String tag, SortDirection direction) {}

}
