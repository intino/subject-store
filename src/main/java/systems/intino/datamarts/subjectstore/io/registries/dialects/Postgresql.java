package systems.intino.datamarts.subjectstore.io.registries.dialects;

import systems.intino.datamarts.subjectstore.io.registries.SqlStatementProvider;

public class Postgresql implements SqlStatementProvider.SqlDialect {
	@Override
	public String get(SqlStatementProvider.StatementType type) {
		return switch (type) {
			case CreateSubjectsTable -> Postgresql.CreateSubjectsTable;
			case SelectSubject -> Postgresql.SelectSubject;
			case CreateSubject -> Postgresql.CreateSubject;
			case InitTables -> Postgresql.InitTables;
			case DropTables -> Postgresql.DropTables;
		};
	}

	private static final String CreateSubjectsTable = """
    CREATE TABLE IF NOT EXISTS subjects (
        id SERIAL PRIMARY KEY,
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
        num DOUBLE PRECISION,
        txt TEXT,
        PRIMARY KEY (feed, tag)
    );
    
    INSERT INTO s[id]_tags (tag, label, feed) VALUES (0, 'ts', -1);
    INSERT INTO s[id]_tags (tag, label, feed) VALUES (1, 'ss', -1);
    
    CREATE INDEX IF NOT EXISTS s[id]_idx_tag ON s[id]_map(tag);
    CREATE INDEX IF NOT EXISTS s[id]_idx_feed ON s[id]_map(feed);
    
    INSERT INTO s[id]_map (tag, feed, num, txt)
    SELECT -1, -1, 0, NULL
    WHERE NOT EXISTS (SELECT 1 FROM s[id]_map);
    """;

	private static final String DropTables = """
    DROP INDEX IF EXISTS s[id]_idx_tag;
    DROP INDEX IF EXISTS s[id]_idx_feed;
    
    DROP TABLE IF EXISTS s[id]_map;
    DROP TABLE IF EXISTS s[id]_tags;
    """;
}
