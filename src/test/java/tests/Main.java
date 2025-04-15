package tests;

import systems.intino.datamarts.subjectstore.SqliteConnection;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.index.model.Subject;

import java.io.File;
import java.sql.Connection;

public class Main {
	public static void main(String[] args) {
		Connection connection = SqliteConnection.from(new File("xx.iss"));
		SubjectIndex index = new SubjectIndex(connection);
		Subject subject = index.create("12345", "patient");
		SubjectHistory history = new SubjectHistory(subject.identifier(), connection);


	}
}
