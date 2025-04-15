package systems.intino.datamarts.subjectstore;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqliteConnection {
	static { loadDriver(); }

	public static Connection from(File file) {
		try {
			return DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static Connection inMemory() {
		try {
			return DriverManager.getConnection("jdbc:sqlite::memory:");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static void loadDriver() {
		try {
			inMemory().close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


}
