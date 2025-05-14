package tests;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class Jdbc {

	// docker run --rm --name mysql-test -e MYSQL_ROOT_PASSWORD=root -e MYSQL_USER=user -e MYSQL_PASSWORD=1234 -p 3306:3306 -d mysql:latest
	public static String mysql() throws Exception {
		return "jdbc:mysql://localhost:3306/" + createDatabase("mysql") + "/?user=root&password=root&useSSL=false&allowPublicKeyRetrieval=true";
	}

	// docker run --rm -d --name mariadb-test -e MARIADB_ROOT_PASSWORD=root -e MARIADB_DATABASE=test -p 3306:3306 -d mariadb:latest
	public static String mariadb() throws Exception {
		return "jdbc:mariadb://localhost:3306/" + createDatabase("mariadb") + "/?user=root&password=root&useSSL=false&allowPublicKeyRetrieval=true";
	}

	// docker run --name pg-test --rm -e POSTGRES_PASSWORD=root -e POSTGRES_USER=root -p 3306:5432 -d postgres:latest
	public static String postgresql() throws Exception {
		return "jdbc:postgresql://localhost:3306/" + createDatabase("postgresql") + "?user=root&password=root";
	}

	public static String sqlite() throws IOException {
		File file = Files.createTempFile("history", "iss").toFile();
		return "jdbc:sqlite:" + file.getAbsolutePath();
	}

	public static String memory() {
		return "jdbc:sqlite::memory:";
	}

	private static String createDatabase(String type) throws SQLException {
		String database = "DB" + UUID.randomUUID().toString().substring(0, 8);
		try (Connection connection = DriverManager.getConnection("jdbc:" + type + "://localhost:3306/?user=root&password=root");
			Statement statement = connection.createStatement()) {
			statement.execute("CREATE DATABASE \"" + database + "\"");
		}
		return database;
	}


}
