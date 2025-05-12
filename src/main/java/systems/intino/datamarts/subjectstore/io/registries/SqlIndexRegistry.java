package systems.intino.datamarts.subjectstore.io.registries;

import systems.intino.datamarts.subjectstore.io.IndexRegistry;

import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlIndexRegistry implements IndexRegistry {
	private final Connection connection;
	private final StatementProvider statementProvider;

	public SqlIndexRegistry(String jdbcUrl) {
		try {
			this.connection = SqlConnection.get(jdbcUrl);
			this.initTables(database(jdbcUrl));
			this.statementProvider = new StatementProvider(database(jdbcUrl));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<String> subjects() {
		try (ResultSet rs = statementProvider.get("select-subjects").executeQuery()) {
			return readList(rs, this::readSubject);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Long> links() {
		try (ResultSet rs = statementProvider.get("select-links").executeQuery()) {
			return readList(rs, this::readLink);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("SqlSourceToSinkFlow")
	@Override
	public List<String> terms(List<Integer> ids) {
		String sql = "SELECT id, tag, value FROM terms WHERE id IN (" + join(ids) + ")";
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
			return readList(rs, SqlIndexRegistry::readTerm);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("SqlSourceToSinkFlow")
	@Override
	public List<String> terms(Set<String> tags) {
		String sql = "SELECT id, tag, value FROM terms WHERE tag IN (" + join(tags) + ")";
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
			return readList(rs, SqlIndexRegistry::readTerm);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int insertSubject(String subject) {
		try {
			PreparedStatement statement = statementProvider.get("insert-subject");
			statement.setString(1, subject);
			statement.executeUpdate();
			ResultSet keys = statement.getGeneratedKeys();
			if (keys.next()) return keys.getInt(1);
			return -1;
		}
		catch (SQLException e) {
			throw new RuntimeException("Failed to insert subject");
		}
	}

	@Override
	public void setSubject(int id, String subject) {
		try {
			PreparedStatement statement = statementProvider.get("update-subject");
			statement.setString(1, subject);
			statement.setInt(2, id);
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deleteSubject(int subject) {
		try {
			{
				PreparedStatement statement = statementProvider.get("delete-subject");
				statement.setInt(1, subject);
				statement.executeUpdate();
			}
			{
				PreparedStatement statement = statementProvider.get("delete-subject-links");
				statement.setInt(1, subject);
				statement.executeUpdate();
			}
			connection.commit();
		}
		catch (SQLException e) {
			throw new RuntimeException("Failed to delete subject");
		}
	}

	@Override
	public int term(String tag, String value) {
		try {
			PreparedStatement statement = statementProvider.get("select-term");
			statement.setString(1, tag);
			statement.setString(2, value);
			ResultSet rs = statement.executeQuery();
			return rs.next() ? rs.getInt(1) : -1;
		}
		catch (SQLException e) {
			throw new RuntimeException("Failed to insert term");
		}
	}

	@Override
	public int insertTerm(String tag, String value) {
		try {
			PreparedStatement statement = statementProvider.get("insert-term");
			statement.setString(1, tag);
			statement.setString(2, value);
			statement.executeUpdate();
			ResultSet rs = statement.getGeneratedKeys();
			return rs.next() ? rs.getInt(1) : -1;
		}
		catch (SQLException e) {
			throw new RuntimeException("Failed to insert term");
		}
	}

	@Override
	public void setTerm(int id, String tag, String value) {
		try {
			PreparedStatement statement = statementProvider.get("update-term");
			statement.setString(1, tag);
			statement.setString(2, value);
			statement.setInt(3, id);
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deleteTerm(int term) {
		try {
			PreparedStatement statement = statementProvider.get("delete-term");
			statement.setInt(1, term);
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void link(int subject, int term) {
		execute(subject, term, "link");
	}

	@Override
	public void unlink(int subject, int term) {
		execute(subject, term, "unlink");
	}

	@Override
	public void commit() {
		try {
			connection.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


	@Override
	public Stream<String> dump() {
		try {
			ResultSet rs = statementProvider.get("select-triples").executeQuery();
			return triplesOf(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean hasHistory(int id) {
		try (ResultSet rs = connection.getMetaData().getTables(null, null, "s" + id + "_tags", null)) {
			return rs.next();
		} catch (SQLException e) {
			return false;
		}
	}

	private String readSubject(ResultSet rs) {
		try {
			return rs.getString(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private Stream<String> triplesOf(ResultSet rs) {
		return Stream.generate(() -> readTriple(rs))
				.takeWhile(Objects::nonNull)
				.onClose(() -> close(rs));
	}

	private static String readTerm(ResultSet rs) {
		try {
			return rs.getInt(1) + "@" + rs.getString(2) + "=" + rs.getString(3);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private Long readLink(ResultSet rs) {
		try {
			int subject = rs.getInt(1);
			int term = rs.getInt(2);
			return (long) subject << 32 | term;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static <T> List<T> readList(ResultSet rs, Function<ResultSet, T> function) throws SQLException {
		List<T> result = new ArrayList<>();
		while (rs.next())
			result.add(function.apply(rs));
		return result;
	}


	private String readTriple(ResultSet rs) {
		try {
			return rs.next() ? rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3): null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private String split(String str) throws SQLException {
		String[] split = str.split("=",2);
		return split[0] + "\t" + split[1];
	}

	private static void close(ResultSet rs) {
		try {
			rs.getStatement().close();
			rs.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void execute(int subject, int term, String order) {
		try {
			PreparedStatement statement = statementProvider.get(order);
			statement.setInt(1, subject);
			statement.setInt(2, term);
			statement.execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static String join(List<Integer> ids) {
		return ids.stream()
				.map(String::valueOf)
				.collect(Collectors.joining(","));
	}

	private static String join(Set<String> tags) {
		return tags.stream()
				.map(t->"'" + t + "'")
				.collect(Collectors.joining(","));
	}


	@Override
	public void close() throws Exception {
		connection.close();
	}

	private class StatementProvider {
		private static final String SelectTriplesSql = """
            SELECT subjects.name AS subject, terms.tag AS tag, terms.value AS value
            FROM links
            JOIN subjects ON subjects.id = links.subject_id
            JOIN terms ON terms.id = links.term_id
            WHERE subjects.name IS NOT NULL AND terms.tag IS NOT NULL
            ORDER BY subjects.id
        """;

		final Map<String, PreparedStatement> statements;

		StatementProvider(String database) throws SQLException {
			this.statements = statements();
		}

		Map<String, PreparedStatement> statements() throws SQLException {
			Map<String, PreparedStatement> statements = new HashMap<>();
			statements.put("select-subjects", create("SELECT name FROM subjects ORDER BY id"));
			statements.put("select-links", create("SELECT subject_id, term_id FROM links"));

			statements.put("insert-subject", create("INSERT INTO subjects (name) VALUES (?)", Statement.RETURN_GENERATED_KEYS));
			statements.put("update-subject", create("UPDATE subjects SET name = ? WHERE id = ?"));
			statements.put("delete-subject", create("UPDATE subjects SET name = NULL WHERE id = ?"));
			statements.put("select-term", create("SELECT id FROM terms WHERE tag = ? AND value = ?"));
			statements.put("insert-term", create("INSERT INTO terms (tag, value) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS));
			statements.put("update-term", create("UPDATE terms SET tag = ?, value = ? WHERE id = ?"));
			statements.put("delete-term", create("DELETE FROM terms WHERE id = ?"));
			statements.put("delete-subject-links", create("DELETE FROM links WHERE subject_id = ?"));
			statements.put("delete-all-terms-of-subject", create("UPDATE terms SET tag = NULL WHERE tag IS NOT NULL AND id IN (SELECT term_id FROM links WHERE subject_id = ? AND term_id NOT IN (SELECT term_id FROM links WHERE subject_id != ?))"));
			statements.put("link", create("INSERT INTO links (subject_id, term_id) VALUES (?, ?)"));
			statements.put("unlink", create("DELETE FROM links WHERE subject_id = ? and term_id = ?"));

			statements.put("select-triples", create(SelectTriplesSql));
			return statements;
		}

		PreparedStatement get(String sql) {
			return statements.get(sql);
		}

		private PreparedStatement create(String sql) throws SQLException {
			return connection.prepareStatement(sql);
		}

		private PreparedStatement create(String sql, int... options) throws SQLException {
			return connection.prepareStatement(sql, options);
		}
	}

	private void initTables(String database) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			String[] statements = SqlIndexTables.get(database).split("\\n\\n");
			for (String ddl : statements) {
				if (ddl.isBlank()) continue;
				try {
					statement.executeUpdate(ddl);
				}
				catch (SQLException ignored) {

				}
			}
		}

		connection.commit();
	}

	private static String database(String jdbcUrl) {
		return jdbcUrl.split(":")[1];
	}

}
