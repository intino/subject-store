package tests;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Storages {
	static { loadDriver(); }

	public static String in(File file) {
		return "jdbc:sqlite:" + file.getAbsolutePath();
	}

	public static String inMemory() {
		return "jdbc:sqlite::memory:";
	}

	private static void loadDriver() {
		try {
			DriverManager.getConnection(inMemory()).close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


}
