package systems.intino.datamarts.subjectstore.io.registries;

public class SqlIndexTables {



	private static final String InitTables = """
			CREATE TABLE IF NOT EXISTS subjects (
				id INTEGER PRIMARY KEY AUTO_INCREMENT,
				name TEXT
			);
			
			CREATE TABLE IF NOT EXISTS terms (
				id INTEGER PRIMARY KEY AUTO_INCREMENT,
				tag TEXT,
				value TEXT
			);
			
			CREATE TABLE IF NOT EXISTS links (
				subject_id INTEGER NOT NULL,
				term_id INTEGER NOT NULL,
				PRIMARY KEY (subject_id, term_id)
			);
			
			CREATE INDEX idx_terms ON terms(tag(100), value(100));
			
			CREATE INDEX idx_terms_tag ON terms(tag(100));
			""";

	private static final String InitTablesSqlite = """
			CREATE TABLE IF NOT EXISTS subjects (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT
			);
			
			CREATE TABLE IF NOT EXISTS terms (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				tag TEXT,
				value TEXT
			);
			
			CREATE TABLE IF NOT EXISTS links (
				subject_id INTEGER NOT NULL,
				term_id INTEGER NOT NULL,
				PRIMARY KEY (subject_id, term_id)
			);
			
			CREATE INDEX idx_terms ON terms(tag, value);
			
			CREATE INDEX idx_terms_tag ON terms(tag);
			""";

	public static String get(String database) {
		if (database.equals("sqlite")) return InitTablesSqlite;
		return InitTables;
	}
}
