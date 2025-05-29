package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Triple;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.model.Term;
import systems.intino.datamarts.subjectstore.view.Stats;
import systems.intino.datamarts.subjectstore.view.index.Column;
import systems.intino.datamarts.subjectstore.view.index.Column.Type;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class SubjectIndexView  {
	private final List<Subject> subjects;
	private final List<Column> columns;

	public static Builder of(List<Subject> subjects) {
		return new Builder(subjects);
	}

	private SubjectIndexView(List<Subject> subjects, List<Column> columns) {
		this.subjects = subjects;
		this.columns = columns;
	}

	public Triple[] get(int index) {
		return triples(subjects.get(index));
	}

	public Triple[] get(int index, String column) {
		Subject subject = subjects.get(index);
		return triples(subject, column).toArray(Triple[]::new);
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

	public Exporting export() {
		return this::exportTo;
	}

	private void exportTo(OutputStream os) {
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
			writer.write(tsv());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public interface Exporting {
		void to(OutputStream os);
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

	private Triple[] triples(Subject subject) {
		return columns.stream()
				.flatMap(s -> triples(subject, s.name()))
				.toArray(Triple[]::new);
	}

	private Stream<Triple> triples(Subject subject, String tag) {
		return termsOf(subject).stream()
				.filter(term->term.is(tag))
				.map(term->new Triple(subject.identifier(), term.tag(), term.value()));
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
		private final List<Column> columns;

		public Builder(List<Subject> subjects) {
			this.subjects = subjects;
			this.columns = new ArrayList<>();
		}

		public Builder add(String tag, Type type) {
			columns.add(new Column() {
				@Override
				public String name() {
					return tag;
				}

				@Override
				public Type type() {
					return type;
				}

				@Override
				public Stats stats() {
					List<String> values = subjects.stream()
							.flatMap(subject -> subject.terms().stream())
							.filter(term -> term.is(tag))
							.map(Term::value)
							.toList();
					return Stats.of(values.toArray(new String[0]));				}
			});

			return this;
		}

		public SubjectIndexView build() {
			return new SubjectIndexView(subjects, columns);
		}

		public SubjectIndexView.Exporting export() {
			return build().export();
		}
	}


}
