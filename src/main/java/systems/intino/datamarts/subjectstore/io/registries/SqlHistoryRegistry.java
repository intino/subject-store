package systems.intino.datamarts.subjectstore.io.registries;


import systems.intino.datamarts.subjectstore.SubjectHistory.RegistryException;
import systems.intino.datamarts.subjectstore.io.HistoryRegistry;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static java.sql.Types.*;
import static systems.intino.datamarts.subjectstore.TimeReferences.*;

public class SqlHistoryRegistry implements HistoryRegistry {
	private final String identifier;
	private final Connection connection;
	private final boolean shared;
	private final int id;
	private final StatementProvider statementProvider;
	private int feedCount;
	private int current;

	public SqlHistoryRegistry(String identifier, String jdbcUrl) {
		try {
			this.identifier = identifier;
			this.connection = SqlConnection.get(jdbcUrl);
			this.shared = jdbcUrl.startsWith("shared:");
			this.id = idOf(identifier);
			this.initTables();
			this.statementProvider = new StatementProvider();
			this.initTags();
			this.feedCount = readFeedCount();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private int idOf(String identifier) {
		try {
			int id = selectIdOf(identifier);
			if (id >= 0) return id;
			connection.createStatement().execute("INSERT OR IGNORE INTO subjects(name) VALUES ('" + identifier + "')");
			return selectIdOf(identifier);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private int selectIdOf(String identifier) throws SQLException {
		try (ResultSet select = selectIdentifier(identifier)) {
			if (select.next()) return select.getInt(1);
			return -1;
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
				return select.getString(1);
			}
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	@Override
	public double getNumber(int tag, int feed)  {
		try (ResultSet select = selectDoubleValue(tag, feed)) {
			return select.getDouble(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getText(int tag, int feed)  {
		try (ResultSet select = selectTextValue(tag, feed)) {
			return select.getString(1);
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
			connection.createStatement().executeUpdate(normalize(DropTables));
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
		return statementProvider.get("select-all").executeQuery();
	}

	private void initTables() throws SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate(normalize(InitTables));
		connection.commit();
	}

	private void initTags() throws SQLException {
		try (ResultSet rs = selectTags()) {
			if (rs.next()) return;
			insertTag(0, "ts");
			insertTag(1, "ss");
			connection.commit();
		}
	}

	private ResultSet selectIdentifier(String identifier) throws SQLException {
		connection.createStatement().execute("CREATE TABLE IF NOT EXISTS subjects (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)");
		connection.commit();
		return connection.createStatement().executeQuery("SELECT id FROM subjects WHERE name = '" + identifier + "'");
	}

	private ResultSet selectTags() throws SQLException {
		return statementProvider.get("select-tags").executeQuery();
	}

	private ResultSet selectInstants() throws SQLException {
		return statementProvider.get("select-instants").executeQuery();
	}

	private ResultSet selectDoubleValue(int tag, int feed) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-double-value");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		return statement.executeQuery();
	}

	private ResultSet selectTextValue(int tag, int feed) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-string-value");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		return statement.executeQuery();
	}

	private ResultSet selectDoubleValues(int tag, int from, int to) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-double-values");
		statement.setInt(1, tag);
		statement.setInt(2, from);
		statement.setInt(3, to);
		return statement.executeQuery();
	}

	private ResultSet selectStringValues(int tag, int from, int to) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-string-values");
		statement.setInt(1, tag);
		statement.setInt(2, from);
		statement.setInt(3, to);
		return statement.executeQuery();
	}

	private void insertTag(int id, String tag) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-tag");
		statement.setInt(1, id);
		statement.setString(2, tag);
		statement.execute();
	}

	private void insertEntry(int tag, int feed, Instant value) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-entry");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		statement.setLong(3, value.toEpochMilli());
		statement.setString(4, labelOf(value));
		statement.execute();
	}

	private void insertEntry(int tag, int feed, double value) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-entry");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		statement.setDouble(3, value);
		statement.setNull(4, VARCHAR);
		statement.execute();
	}

	private void insertEntry(int tag, int feed, String value) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-entry");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		statement.setNull(3, BIGINT);
		statement.setString(4, value);
		statement.execute();
	}

	private void updateSize(int size) throws SQLException {
		PreparedStatement statement = statementProvider.get("update-feed");
		statement.setInt(1, size);
		statement.execute();
	}

	private void updateTag(int tag, int feed) throws SQLException {
		PreparedStatement statement = statementProvider.get("update-tag-feed");
		statement.setInt(1, feed);
		statement.setInt(2, tag);
		statement.execute();
	}

	private static String labelOf(Instant value) {
		if (value.equals(Legacy)) return "Legacy";
		if (value.equals(BigBang)) return "Big Bang";
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

	static final String InitTables = """
		CREATE TABLE IF NOT EXISTS s[id]_tags (
			tag INTEGER NOT NULL,
			label TEXT,
			feed INTEGER,
			PRIMARY KEY (tag)
		);
	
		CREATE TABLE IF NOT EXISTS s[id]_map (
			feed INTEGER NOT NULL,
			tag INTEGER NOT NULL,
			num REAL,
			txt TEXT,
			PRIMARY KEY (feed, tag)
		);
	
		CREATE INDEX IF NOT EXISTS s[id]_idx_tag ON s[id]_map(tag);
		CREATE INDEX IF NOT EXISTS s[id]_idx_feed ON s[id]_map(feed);
		INSERT INTO s[id]_map (tag, feed, num, txt) SELECT -1, -1, 0, NULL WHERE NOT EXISTS (SELECT 1 FROM s[id]_map);
		""";

	static final String DropTables = """
			DROP INDEX IF EXISTS s[id]_idx_tag;
			DROP INDEX IF EXISTS s[id]_idx_feed;
			
			DROP TABLE IF EXISTS s[id]_map;
			DROP TABLE IF EXISTS s[id]_tags;
			""";

	@Override
	public void close() throws Exception {
		if (shared) return;
		connection.close();
	}

	private class StatementProvider {

		final Map<String, PreparedStatement> statements;

		StatementProvider() throws SQLException {
			this.statements = statements();
		}

		Map<String, PreparedStatement> statements() throws SQLException {
			Map<String, PreparedStatement> statements = new HashMap<>();
			statements.put("insert-tag", create("INSERT INTO s[id]_tags (tag, label, feed) VALUES (?, ?, -1)"));
			statements.put("insert-entry", create("INSERT INTO s[id]_map (tag, feed, num, txt) VALUES (?, ?, ?, ?)"));
			statements.put("update-tag-feed", create("UPDATE s[id]_tags SET feed = ? WHERE tag = ?;"));
			statements.put("update-feed", create("UPDATE s[id]_map SET num = ? WHERE tag = -1 AND feed = -1;"));
			statements.put("select-all", create("SELECT * FROM s[id]_map ORDER BY feed, tag;"));
			statements.put("select-tags", create("SELECT tag, label, feed FROM s[id]_tags"));
			statements.put("select-instants", create("SELECT feed, num FROM s[id]_map WHERE tag = 0"));
			statements.put("select-double-value", create("SELECT num FROM s[id]_map WHERE tag = ? AND feed = ?"));
			statements.put("select-double-values", create("SELECT feed, num FROM s[id]_map WHERE tag = ? and feed BETWEEN ? AND ?"));
			statements.put("select-string-value", create("SELECT txt FROM s[id]_map WHERE tag = ? AND feed = ?"));
			statements.put("select-string-values", create("SELECT feed, txt FROM s[id]_map WHERE tag = ? and feed BETWEEN ? AND ?"));
			return statements;
		}

		PreparedStatement get(String sql) {
			return statements.get(sql);
		}

		private PreparedStatement create(String sql) throws SQLException {
			return connection.prepareStatement(normalize(sql));
		}

	}

	private String normalize(String sql) {
		return sql.replace("[id]", String.valueOf(id));
	}
	@Override
	public String toString() {
		return identifier;
	}
}
