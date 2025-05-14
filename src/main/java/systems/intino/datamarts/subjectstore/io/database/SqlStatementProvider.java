package systems.intino.datamarts.subjectstore.io.database;

import systems.intino.datamarts.subjectstore.io.database.dialects.MySql;
import systems.intino.datamarts.subjectstore.io.database.dialects.Postgresql;
import systems.intino.datamarts.subjectstore.io.database.dialects.Sqlite;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static systems.intino.datamarts.subjectstore.io.database.SqlStatementProvider.StatementType.*;

@SuppressWarnings("SqlSourceToSinkFlow")
public class SqlStatementProvider {
	private final Connection connection;
	private final SqlDialect dialect;
	private int id;

	public SqlStatementProvider(Connection connection) throws SQLException {
		this.connection = connection;
		this.dialect = dialectOf(dbOf(connection));
	}

	public SqlStatementProvider of(String identifier) throws SQLException {
		this.id = idOf(identifier);
		return this;
	}

	private int idOf(String identifier) {
		try {
			int id = getSubjectId(identifier);
			return id >= 0 ? id : createSubject(identifier);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private int getSubjectId(String identifier) throws SQLException {
		checkSubjectsTable();
		try (PreparedStatement statement = connection.prepareStatement(dialect.get(SelectSubject))) {
			statement.setString(1, identifier);
			try (ResultSet rs = statement.executeQuery()) {
				return rs.next() ? rs.getInt(1) : -1;
			}
		}
	}

	private int createSubject(String identifier) throws SQLException {
		this.id = insertSubject(identifier);
		this.initTables();
		this.connection.commit();
		return id;
	}

	private int insertSubject(String identifier) throws SQLException {
		String sql = dialect.get(CreateSubject);
		try (PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			statement.setString(1, identifier);
			statement.executeUpdate();
			try (ResultSet rs = statement.getGeneratedKeys()) {
				return rs.next() ? rs.getInt(1) : -1;
			}
		}
	}

	private void initTables() throws SQLException {
		try (Statement statement = connection.createStatement()) {
			String[] sentences = customize(dialect.get(InitTables)).split(";\n");
			for (String sentence : sentences) {
				statement.execute(sentence);
			}
		}
	}

	public Map<String, PreparedStatement> statements() throws SQLException {
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
		statements.put("select-last-values", create("SELECT t.feed, t.tag, m.num, m.txt FROM s[id]_tags t JOIN s[id]_map m ON t.feed = m.feed AND t.tag = m.tag;"));
		statements.put("drop-tables", create(dialect.get(DropTables)));
		return statements;
	}

	private PreparedStatement create(String sql) throws SQLException {
		return connection.prepareStatement(customize(sql));
	}

	private String customize(String sql) {
		return sql.replace("[id]", String.valueOf(id));
	}

	private static SqlDialect dialectOf(String db) throws SQLException {
		if (db.equalsIgnoreCase("sqlite")) return new Sqlite();
		if (db.equalsIgnoreCase("mysql") || db.equalsIgnoreCase("mariadb")) return new MySql();
		if (db.equalsIgnoreCase("postgresql") ) return new Postgresql();
		throw new SQLException("Not supported " + db);
	}

	private static String dbOf(Connection connection) throws SQLException {
		return connection.getMetaData().getDatabaseProductName();
	}

	private void checkSubjectsTable() throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(dialect.get(CreateSubjectsTable));
			connection.commit();
		}
	}

	public interface SqlDialect {
		String get(StatementType type);
	}

	public enum StatementType {
		CreateSubjectsTable, SelectSubject, CreateSubject, InitTables, DropTables
	}


}
