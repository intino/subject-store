package systems.intino.datamarts.subjectstore;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SubjectHistoryVault implements Closeable {
	private final Connection connection;

	public SubjectHistoryVault(String jdbcUrl) {
		this.connection = connection(jdbcUrl);
	}

	public SubjectHistory get(String subject) {
		return new SubjectHistory(subject, connection);
	}

	private Connection connection(String jdbcUrl) {
		try {
			Connection connection = DriverManager.getConnection(jdbcUrl);
			connection.setAutoCommit(false);
			return connection;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
