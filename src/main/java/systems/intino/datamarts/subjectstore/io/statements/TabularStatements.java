package systems.intino.datamarts.subjectstore.io.statements;

import systems.intino.datamarts.subjectstore.io.Statements;
import systems.intino.datamarts.subjectstore.model.Triple;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.model.Term;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

public class TabularStatements implements Statements, AutoCloseable {
	private final BufferedReader reader;
	private final TabularSchema header;
	private final String separator;

	public TabularStatements(InputStream is, String separator) {
		this.reader = new BufferedReader(new InputStreamReader(is));
		this.separator = separator;
		this.header = new TabularSchema(nextLine());
	}

	@Override
	public Schema schema() {
		return header;
	}

	@Override
	public Iterator<Triple> iterator() {
		return new Iterator<>() {
			Iterator<Triple> row = nextRow();

			@Override
			public boolean hasNext() {
				return row.hasNext();
			}

			@Override
			public Triple next() {
				try {
					return row.next();
				} finally {
					if (!row.hasNext()) row = nextRow();
				}
			}
		};
	}

	private Iterator<Triple> nextRow() {
		return header.statementsIn(nextLine());
	}

	private String[] nextLine() {
		try {
			String line = reader.readLine();
			return line != null ? line.split(separator) : new String[0];
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	static class TabularSchema implements Schema {
		private final String[] fields;
		private final int id;
		private final Map<String, Function<String,String>> mappers;

		public TabularSchema(String[] fields) {
			this.fields = fields;
			this.id = IntStream.range(0, fields.length).filter(i->fields[i].equals("id")).findFirst().orElse(0);
			this.mappers = new HashMap<>();
		}

		@Override
		public TabularSchema map(String name, Function<String,String> mapper) {
			mappers.put(name, mapper);
			return this;
		}

		private Subject subject(String[] row) {
			return Subject.of(mapperOf("id").apply(row[this.id]));
		}

		private Function<String, String> mapperOf(String id) {
			return mappers.getOrDefault(id, s -> s);
		}

		private Term[] terms(String[] values) {
			return IntStream.range(0, values.length)
					.filter(i -> i != id)
					.filter(i -> !values[i].isEmpty())
					.mapToObj(i -> term(fields[i], mapperOf(fields[i]).apply(values[i])))
					.filter(Objects::nonNull)
					.toArray(Term[]::new);
		}

		private Term term(String field, String value) {
			return value != null ? new Term(field, value) : null;
		}

		private Iterator<Triple> statementsIn(String[] values) {
			if (values.length == 0) return Collections.emptyIterator();
			return new Iterator<>() {
				final Subject subject = subject(values);
				final Term[] terms = terms(values);
				int i = 0;

				@Override
				public boolean hasNext() {
					return i < terms.length;
				}

				@Override
				public Triple next() {
					return new Triple(subject, terms[i++]);
				}
			};
		}
	}

}
