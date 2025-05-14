package systems.intino.datamarts.subjectstore.io.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SqlConnection {
	private static final Map<String, Connection> connections = new HashMap<>();

	public static Connection get(String url) {
		return open(url);
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
				connections.put(url, connection);
				return connection;
	}

	private static boolean isValid(Connection connection) throws SQLException {
		return connection != null && !connection.isClosed();
	}

}
