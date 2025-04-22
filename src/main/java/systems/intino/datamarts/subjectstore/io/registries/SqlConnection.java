package systems.intino.datamarts.subjectstore.io.registries;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class SqlConnection {
	private static final Map<String, Connection> connections = new HashMap<>();
	private static final String SHARED = "shared:";

	public static Connection get(String jdbcUrl) {
		String url = unwrap(jdbcUrl);
		Connection connection = open(url);
		connections.put(url, connection);
		return connection;
	}

	private static Connection open(String url)  {
		try {
			Connection connection = connections.get(url);
			return isValid(connection) ? connection : create(url);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static Connection create(String url) throws SQLException {
		Connection connection = DriverManager.getConnection(url);
		connection.setAutoCommit(false);
		return connection;
	}

	private static boolean isValid(Connection connection) throws SQLException {
		return connection != null && !connection.isClosed();
	}

	private static String unwrap(String storage) {
		return storage.startsWith(SHARED) ? storage.substring(SHARED.length()) : storage;
	}

	public static String shared(String jdbcUrl) {
		return SHARED + jdbcUrl;
	}
}
