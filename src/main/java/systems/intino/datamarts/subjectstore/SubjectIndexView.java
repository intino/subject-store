package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Statement;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.view.Column;
import systems.intino.datamarts.subjectstore.model.Term;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class SubjectIndexView implements Iterable<Column.StringColumn>  {
	private final List<Subject> subjects;
	private final List<String> tags;

	public static Builder of(List<Subject> subjects) {
		return new Builder(subjects);
	}

	private SubjectIndexView(List<Subject> subjects, List<String> tags) {
		this.subjects = subjects;
		this.tags = tags;
	}

	public Statement[] get(int index) {
		return statements(subjects.get(index));
	}

	public Statement[] get(int index, String column) {
		Subject subject = subjects.get(index);
		return statements(subject, column).toArray(Statement[]::new);
	}

	public int size() {
		return subjects.size();
	}

	public List<String> rows() {
		return subjects.stream().map(Subject::identifier).toList();
	}

	public List<String> columns() {
		return tags;
	}

	public Column.StringColumn column(String name) {
		return new Column.StringColumn(name, valuesOf(name));
	}

	private String[] valuesOf(String key) {
		return subjects.stream()
				.flatMap(s-> statements(s, key))
				.map(s->s.term().value())
				.toArray(String[]::new);
	}

	@Override
	public Iterator<Column.StringColumn> iterator() {
		return new Iterator<>() {
			private final Iterator<String> iterator = tags.iterator();

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Column.StringColumn next() {
				return column(iterator.next());
			}
		};
	}

	public void exportTo(File file) throws IOException {
		try (OutputStream os = new FileOutputStream(file)) {
			exportTo(os);
		}
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
			for (String column : columns())
				line.add(value(subject, column));
			sb.append(line).append('\n');
		}
		return sb.toString();
	}

	private Statement[] statements(Subject subject) {
		return tags.stream()
				.flatMap(s -> statements(subject, s))
				.toArray(Statement[]::new);
	}

	private Stream<Statement> statements(Subject subject, String tag) {
		return termsOf(subject).stream()
				.filter(term->term.is(tag))
				.map(term->new Statement(subject, term));
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
		private final List<Subject> subjects;
		private final List<String> tags;

		public Builder(List<Subject> subjects) {
			this.subjects = subjects;
			this.tags = new ArrayList<>();
		}

		public Builder add(String tag) {
			tags.add(tag);
			return this;
		}

		public SubjectIndexView build() {
			return new SubjectIndexView(subjects, tags);
		}

	}

}
