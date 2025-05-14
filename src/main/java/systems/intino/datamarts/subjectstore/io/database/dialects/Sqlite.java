package systems.intino.datamarts.subjectstore.io.database.dialects;

import systems.intino.datamarts.subjectstore.io.database.SqlStatementProvider;

public class Sqlite implements SqlStatementProvider.SqlDialect {
	@Override
	public String get(SqlStatementProvider.StatementType type) {
		return switch (type) {
			case CreateSubjectsTable -> Sqlite.CreateSubjectsTable;
			case SelectSubject -> Sqlite.SelectSubject;
			case CreateSubject -> Sqlite.CreateSubject;
			case InitTables -> Sqlite.InitTables;
			case DropTables -> Sqlite.DropTables;
		};
	}

	private static final String CreateSubjectsTable = """
	CREATE TABLE IF NOT EXISTS subjects (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT UNIQUE
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
	
	INSERT INTO s[id]_tags (tag, label, feed) VALUES (0, 'ts', -1);
	INSERT INTO s[id]_tags (tag, label, feed) VALUES (1, 'ss', -1);
	
	CREATE INDEX IF NOT EXISTS s[id]_idx_tag ON s[id]_map(tag);
	CREATE INDEX IF NOT EXISTS s[id]_idx_feed ON s[id]_map(feed);
	INSERT INTO s[id]_map (tag, feed, num, txt) SELECT -1, -1, 0, NULL WHERE NOT EXISTS (SELECT 1 FROM s[id]_map);
	""";

	private static final String DropTables = """
	DROP INDEX IF EXISTS s[id]_idx_tag;
	DROP INDEX IF EXISTS s[id]_idx_feed;
	
	DROP TABLE IF EXISTS s[id]_map;
	DROP TABLE IF EXISTS s[id]_tags;
	""";
}
