package tests;

import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectStore;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.model.Subjects;

import static systems.intino.datamarts.subjectstore.TimeReference.today;

public class Main {
	public static void main(String[] args) {
		try (SubjectStore store = new SubjectStore(Storages.inMemory())) {
			Subject subject = store.create("12345", "patient");
			subject.rename("lewis.carroll");
			subject.update()
					.set("name", "lewis")
					.set("age", 20)
					.put("email", "lewis@carroll.com")
					.commit();

			try (SubjectHistory history = subject.history()) {
				history.on(today(), "ss")
						.put("height", 123)
						.commit();
			}
			subject.drop();

			Subjects subjects = store.subjects("patient").where("name").contains("lew");
		}
	}
}
