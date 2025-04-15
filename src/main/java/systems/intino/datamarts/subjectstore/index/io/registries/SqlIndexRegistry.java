package systems.intino.datamarts.subjectstore.index.io.registries;

import systems.intino.datamarts.subjectstore.index.io.IndexRegistry;

import java.sql.*;
import java.util.*;
import java.util.function.IntPredicate;
import java.util.stream.Stream;

public class SqlIndexRegistry implements IndexRegistry {
	private final Connection connection;
	private final StatementProvider statementProvider;

	public SqlIndexRegistry(Connection connection) {
		try {
			this.connection = connection;
			this.connection.setAutoCommit(false);
			this.initTables();
			this.statementProvider = new StatementProvider();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<String> subjects() {
		try (ResultSet rs = selectSubjects()) {
			return readStrings(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<String> terms() {
		try (ResultSet rs = selectTerms()) {
			return readStrings(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void rename(int id, String name) {
		try {
			PreparedStatement statement = statementProvider.get("rename-subject");
			statement.setString(1, name);
			statement.setInt(2, id);
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
	public List<Integer> termsOf(int subject) {
		try (ResultSet rs = selectTermsOf(subject)) {
			return readIntegers(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Integer> exclusiveTermsOf(int subject) {
		try (ResultSet rs = selectExclusiveTermsOf(subject)) {
			return readIntegers(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Integer> subjectsFilteredBy(List<Integer> subjects, List<Integer> terms) {
		try (ResultSet rs = selectSubjectsWith(subjects, terms)) {
			return readIntegers(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static List<String> readStrings(ResultSet rs) throws SQLException {
		List<String> result = new ArrayList<>();
		while (rs.next())
			result.add(rs.getString(1));
		return result;
	}

	private static List<Integer> readIntegers(ResultSet rs) throws SQLException {
		List<Integer> result = new ArrayList<>();
		while (rs.next())
			result.add(rs.getInt(1));
		return result;
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
			ResultSet rs = statementProvider.get("select-statements").executeQuery();
			return streamOf(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static Stream<String> streamOf(ResultSet rs) {
		return Stream.generate(() -> readFrom(rs))
				.takeWhile(Objects::nonNull)
				.onClose(() -> close(rs));
	}

	private static String readFrom(ResultSet rs) {
		try {
			return rs.next() ? rs.getString(1) + "\t" + rs.getString(2) : null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static void close(ResultSet rs) {
		try {
			rs.getStatement().close();
			rs.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private ResultSet selectTermsOf(int subject) throws SQLException {
		PreparedStatement statement = statementProvider.get("get-subject-terms");
		statement.setInt(1, subject);
		statement.executeQuery();
		return statement.executeQuery();
	}

	private ResultSet selectExclusiveTermsOf(int subject) throws SQLException {
		PreparedStatement statement = statementProvider.get("get-exclusive-subject-terms");
		statement.setInt(1, subject);
		statement.setInt(2, subject);
		statement.executeQuery();
		return statement.executeQuery();
	}

	private ResultSet selectSubjectsWith(List<Integer> subjects, List<Integer> terms) throws SQLException {
		String sql = sqlFor(subjects, terms);
		PreparedStatement statement = connection.prepareStatement(sql);
		return statement.executeQuery();
	}

	private static final String SELECT_SUBJECTS = "SELECT DISTINCT subject_id FROM links WHERE subject_id in ";
	private static String sqlFor(List<Integer> subjects, List<Integer> terms) {
		return (SELECT_SUBJECTS + placeholders(subjects) + exists(terms) + notExists(terms))
				.replace("links AND", "links WHERE");
	}

	private static String exists(List<Integer> terms) {
		int[] items = positive(terms);
		return items.length > 0 ? " AND subject_id IN (SELECT subject_id FROM links WHERE term_id IN " + placeholders(items) + ")" : "";
	}

	private static String notExists(List<Integer> terms) {
		int[] items = negative(terms);
		return items.length > 0 ? " AND subject_id NOT IN (SELECT subject_id FROM links WHERE term_id IN (" + placeholders(items) + "))" : "";
	}

	private static int[] positive(List<Integer> terms) {
		return filter(terms, i -> i > 0);
	}

	private static int[] negative(List<Integer> terms) {
		return filter(terms, i -> i < 0);
	}

	private static int[] filter(List<Integer> terms, IntPredicate filter) {
		return terms.stream().mapToInt(i -> i).filter(filter).map(Math::abs).toArray();
	}

	private static String placeholders(int[] array) {
		return Arrays.toString(array).replace('[', '(').replace(']', ')');
	}

	private static String placeholders(List<Integer> list) {
		return placeholders(list.stream().mapToInt(Integer::intValue).toArray());
	}

	private void execute(int subject, int term, String order) {
		try {
			PreparedStatement statement = statementProvider.get(order);
			statement.setInt(1, subject);
			statement.setInt(2, term);
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private ResultSet selectSubjects() throws SQLException {
		PreparedStatement statement = statementProvider.get("select-subject");
		return statement.executeQuery();
	}

	private ResultSet selectTerms() throws SQLException {
		PreparedStatement statement = statementProvider.get("select-terms");
		return statement.executeQuery();
	}

	@Override
	public int insertSubject(String name) {
		try {
			PreparedStatement statement = statementProvider.get("insert-subject");
			statement.setString(1, name);
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
	public int insertTerm(String name) {
		try {
			PreparedStatement statement = statementProvider.get("insert-term");
			statement.setString(1, name);
			statement.executeUpdate();
			ResultSet keys = statement.getGeneratedKeys();
			if (keys.next()) return keys.getInt(1);
			return -1;
		}
		catch (SQLException e) {
			throw new RuntimeException("Failed to insert term");
		}
	}

	@Override
	public void drop(int subject) {
		try {
			{
				PreparedStatement statement = statementProvider.get("delete-subject");
				statement.setInt(1, subject);
				statement.executeUpdate();
			}
			{
				PreparedStatement statement = statementProvider.get("delete-subject-terms");
				statement.setInt(1, subject);
				statement.setInt(2, subject);
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
			throw new RuntimeException("Failed to drop subject");
		}
	}

	private class StatementProvider {
		private static final String SelectStatementSQL = """
            SELECT subjects.name AS name, terms.name AS term
            FROM links
            JOIN subjects ON subjects.id = links.subject_id
            JOIN terms ON terms.id = links.term_id
            WHERE subjects.name IS NOT NULL AND terms.name IS NOT NULL
            ORDER BY subjects.id
        """;

		final Map<String, PreparedStatement> statements;

		StatementProvider() throws SQLException {
			this.statements = statements();
		}

		Map<String, PreparedStatement> statements() throws SQLException {
			Map<String, PreparedStatement> statements = new HashMap<>();
			statements.put("select-subject", create("SELECT name FROM subjects ORDER BY id"));
			statements.put("select-terms", create("SELECT name FROM terms ORDER BY id"));
			statements.put("select-statements", create(SelectStatementSQL));
			statements.put("get-subject-terms", create("SELECT term_id FROM links WHERE subject_id = ?"));
			statements.put("get-exclusive-subject-terms", create("SELECT id FROM terms WHERE name IS NOT NULL AND id IN (SELECT term_id FROM links WHERE subject_id = ? AND term_id NOT IN (SELECT term_id FROM links WHERE subject_id != ?))"));
			statements.put("insert-subject", create("INSERT INTO subjects (name) VALUES (?)"));
			statements.put("insert-term", create("INSERT INTO terms (name) VALUES (?)"));
			statements.put("rename-subject", create("UPDATE subjects SET name = ? WHERE id = ?"));
			statements.put("delete-subject", create("UPDATE subjects SET name = NULL WHERE id = ?"));
			statements.put("delete-subject-links", create("DELETE FROM links WHERE subject_id = ?"));
			statements.put("delete-subject-terms", create("UPDATE terms SET name = NULL WHERE name IS NOT NULL AND id IN (SELECT term_id FROM links WHERE subject_id = ? AND term_id NOT IN (SELECT term_id FROM links WHERE subject_id != ?))"));
			statements.put("link", create("INSERT OR IGNORE INTO links (subject_id, term_id) VALUES (?, ?)"));
			statements.put("unlink", create("DELETE FROM links WHERE subject_id = ? and term_id = ?"));
			return statements;
		}

		PreparedStatement get(String sql) {
			return statements.get(sql);
		}

		private PreparedStatement create(String sql) throws SQLException {
			return connection.prepareStatement(sql);
		}

	}

	private static final String InitTables = """
			CREATE TABLE IF NOT EXISTS subjects (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT
			);
			
			CREATE TABLE IF NOT EXISTS terms (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT
			);
			
			CREATE TABLE IF NOT EXISTS links (
				subject_id INTEGER NOT NULL,
				term_id INTEGER NOT NULL,
				PRIMARY KEY (subject_id, term_id)
			);
			
			CREATE INDEX IF NOT EXISTS idx_subject ON links(subject_id);
			CREATE INDEX IF NOT EXISTS idx_term ON links(term_id);
			""";

	private void initTables() throws SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate(InitTables);
		connection.commit();
	}

}
