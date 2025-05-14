package systems.intino.datamarts.subjectstore.io.database.dialects;

import systems.intino.datamarts.subjectstore.io.database.SqlStatementProvider;

public class MySql implements SqlStatementProvider.SqlDialect {
	@Override
	public String get(SqlStatementProvider.StatementType type) {
		return switch (type) {
			case CreateSubjectsTable -> MySql.CreateSubjectsTable;
			case SelectSubject -> MySql.SelectSubject;
			case CreateSubject -> MySql.CreateSubject;
			case InitTables -> MySql.InitTables;
			case DropTables -> MySql.DropTables;
		};
	}

	private static final String CreateSubjectsTable = """
	CREATE TABLE IF NOT EXISTS subjects (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) UNIQUE
	)
	""";

	private static final String SelectSubject = """
	SELECT id FROM subjects WHERE name = ?
	""";

	private static final String CreateSubject = """
	INSERT INTO subjects(name) VALUES (?)
	""";

	private static final String InitTables = """
	CREATE TABLE IF NOT EXISTS s[id]_tags (
		tag INT NOT NULL,
		label VARCHAR(255),
		feed INT,
		PRIMARY KEY (tag)
	);
	
	CREATE TABLE IF NOT EXISTS s[id]_map (
		feed INT NOT NULL,
		tag INT NOT NULL,
		num DOUBLE,
		txt TEXT,
		PRIMARY KEY (feed, tag)
	);
	
	INSERT INTO s[id]_tags (tag, label, feed) VALUES (0, 'ts', -1);
	INSERT INTO s[id]_tags (tag, label, feed) VALUES (1, 'ss', -1);
	
	CREATE INDEX s[id]_idx_tag ON s[id]_map(tag);
	CREATE INDEX s[id]_idx_feed ON s[id]_map(feed);
	INSERT INTO s[id]_map (tag, feed, num, txt)
		SELECT -1, -1, 0, NULL FROM DUAL
		WHERE NOT EXISTS (SELECT 1 FROM s[id]_map);
	""";

	private static final String DropTables = """
	DROP INDEX IF EXISTS s[id]_idx_tag ON s[id]_map;
	DROP INDEX IF EXISTS s[id]_idx_feed ON s[id]_map;
	
	DROP TABLE IF EXISTS s[id]_map;
	DROP TABLE IF EXISTS s[id]_tags;
	""";
}
