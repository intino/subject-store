package systems.intino.datamarts.subjectstore.io.registries;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SqlStorage {
	private static final Map<String, Connection> connections = new HashMap<>();
	private static final String SHARED = "shared:";


	public static String shared(String storage) {
		return SHARED + storage;
	}

	public static Connection create(String storage) {
		try {
			Connection connection = get(unwrap(storage));
			return connection != null ? connection : register(unwrap(storage));
		} catch (SQLException e) {
			return null;
		}
	}

	private static Connection register(String storage) {
		Connection connection = init(storage);
		connections.put(storage, connection);
		return connection;
	}

	private static Connection get(String storage) throws SQLException {
		Connection connection = connections.get(unwrap(storage));
		return connection != null && !connection.isClosed() ? connection : null;
	}

	private static Connection init(String storage) {
		try {
			Connection connection = DriverManager.getConnection(storage);
			connection.setAutoCommit(false);
			return connection;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static String unwrap(String storage) {
		return storage.startsWith(SHARED) ? storage.substring(SHARED.length()) : storage;
	}
}
