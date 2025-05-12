package systems.intino.datamarts.subjectstore.io.registries;


import systems.intino.datamarts.subjectstore.SubjectHistory.RegistryException;
import systems.intino.datamarts.subjectstore.io.HistoryRegistry;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static java.sql.Types.*;

public class SqlHistoryRegistry implements HistoryRegistry {
	private final String identifier;
	private final Connection connection;
	private final Map<String, PreparedStatement> statements;
	private int feedCount;
	private int current;

	public SqlHistoryRegistry(String identifier, String jdbcUrl) {
		try {
			this.identifier = identifier;
			this.connection = SqlConnection.get(jdbcUrl);
			this.statements = new SqlStatementProvider(connection).of(identifier).statements();
			this.feedCount = readFeedCount();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int size() {
		return feedCount;
	}

	public Stream<Row> tags() {
		try {
			return streamOf(selectTags());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Stream<Row> instants() {
		try {
			return streamOf(selectInstants());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String ss(int feed) {
		try {
			try (ResultSet select = selectTextValue(1, feed)) {
				return select.next() ? select.getString(1) : null;
			}
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	@Override
	public double getNumber(int tag, int feed)  {
		try (ResultSet select = selectDoubleValue(tag, feed)) {
			return select.next() ? select.getDouble(1) : 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getText(int tag, int feed)  {
		try (ResultSet select = selectTextValue(tag, feed)) {
			return select.next() ? select.getString(1) : null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Stream<Row> current() {
		try {
			return streamOf(selectLastValues());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public Stream<Row> getNumbers(int tag, int from, int to) {
		try {
			return streamOf(selectDoubleValues(tag, from, to));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Stream<Row> getTexts(int tag, int from, int to) {
		try {
			return streamOf(selectStringValues(tag, from, to));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setTag(int id, String label) {
		try {
			insertTag(id, label);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setTagLastFeed(int id, int feed) {
		try {
			updateTag(id, feed);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int nextFeed() {
		current = feedCount++;
		return current;
	}

	@Override
	public void put(int tag, Object o) {
		if (current < 0) return;
		try {
			switch (type(o)) {
				case NUMERIC -> insertEntry(tag, current, ((Number) o).doubleValue());
				case DATE -> insertEntry(tag, current, ((Instant) o));
				case VARCHAR -> insertEntry(tag, current, o.toString());
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void commit() {
		try {
			updateSize(feedCount);
			current = -1;
			connection.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void drop() {
		try {
			statements.get("drop").execute();
			connection.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Stream<Row> dump() {
		try {
			return streamOf(selectAll()).skip(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static final int FEEDS = -1;

	private int readFeedCount() throws SQLException {
		return (int) getNumber(FEEDS, FEEDS);
	}

	private int type(Object o) {
		if (o instanceof Number) return NUMERIC;
		if (o instanceof Instant) return DATE;
		return VARCHAR;
	}

	private ResultSet selectAll() throws SQLException {
		return statements.get("select-all").executeQuery();
	}

	private ResultSet selectTags() throws SQLException {
		return statements.get("select-tags").executeQuery();
	}

	private ResultSet selectInstants() throws SQLException {
		return statements.get("select-instants").executeQuery();
	}

	private ResultSet selectDoubleValue(int tag, int feed) throws SQLException {
		PreparedStatement statement = statements.get("select-double-value");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		return statement.executeQuery();
	}

	private ResultSet selectTextValue(int tag, int feed) throws SQLException {
		PreparedStatement statement = statements.get("select-string-value");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		return statement.executeQuery();
	}

	private ResultSet selectDoubleValues(int tag, int from, int to) throws SQLException {
		PreparedStatement statement = statements.get("select-double-values");
		statement.setInt(1, tag);
		statement.setInt(2, from);
		statement.setInt(3, to);
		return statement.executeQuery();
	}

	private ResultSet selectStringValues(int tag, int from, int to) throws SQLException {
		PreparedStatement statement = statements.get("select-string-values");
		statement.setInt(1, tag);
		statement.setInt(2, from);
		statement.setInt(3, to);
		return statement.executeQuery();
	}

	private ResultSet selectLastValues() throws SQLException {
		PreparedStatement statement = statements.get("select-last-values");
		return statement.executeQuery();
	}

	private void insertTag(int id, String tag) throws SQLException {
		PreparedStatement statement = statements.get("insert-tag");
		statement.setInt(1, id);
		statement.setString(2, tag);
		statement.execute();
	}

	private void insertEntry(int tag, int feed, Instant value) throws SQLException {
		PreparedStatement statement = statements.get("insert-entry");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		statement.setLong(3, value.toEpochMilli());
		statement.setString(4, labelOf(value));
		statement.execute();
	}

	private void insertEntry(int tag, int feed, double value) throws SQLException {
		PreparedStatement statement = statements.get("insert-entry");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		statement.setDouble(3, value);
		statement.setNull(4, VARCHAR);
		statement.execute();
	}

	private void insertEntry(int tag, int feed, String value) throws SQLException {
		PreparedStatement statement = statements.get("insert-entry");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		statement.setNull(3, BIGINT);
		statement.setString(4, value);
		statement.execute();
	}

	private void updateSize(int size) throws SQLException {
		PreparedStatement statement = statements.get("update-feed");
		statement.setInt(1, size);
		statement.execute();
	}

	private void updateTag(int tag, int feed) throws SQLException {
		PreparedStatement statement = statements.get("update-tag-feed");
		statement.setInt(1, feed);
		statement.setInt(2, tag);
		statement.execute();
	}

	private static String labelOf(Instant value) {
		return value.toString().substring(0, 19).replace('T', ' ');
	}

	private Stream<Row> streamOf(ResultSet rs)  {
		return Stream.generate(() -> recordIn(rs))
				.takeWhile(Objects::nonNull)
				.onClose(() -> close(rs));
	}

	private Row recordIn(ResultSet rs) {
		return nextIn(rs) ? read(rs) : null;
	}

	private static void close(ResultSet rs) {
		try {
			rs.getStatement().close();
			rs.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static Row read(ResultSet rs) {
		return index -> new Row.Data() {
			@Override
			public int asInt() {
				try {
					return rs.getInt(index);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public long asLong() {
				try {
					return rs.getLong(index);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public double asDouble() {
				try {
					return rs.getDouble(index);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public Instant asInstant() {
				return Instant.ofEpochMilli(asLong());
			}

			@Override
			public String asString() {
				try {
					String value = rs.getString(index);
					return rs.wasNull() ? null : value;
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}

		};
	}

	private boolean nextIn(ResultSet rs) {
		try {
			return rs.next();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws Exception {
		connection.close();
	}


	@Override
	public String toString() {
		return identifier;
	}
}
