package tests.index;

import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.*;
import java.util.List;

public class SubjectIndexLoad_ {

	public static void main(String[] args) throws Exception {
		File file = File.createTempFile("index", "journal");
		long start = System.nanoTime();
		BufferedInputStream is = is();
		SubjectIndex index = new SubjectIndex(file).restore(is);
		long stop1 = System.nanoTime();
		System.out.println((stop1 - start)/1e9);
		Thread.sleep(1000);
		List<Subject> subjects = index.subjects()
				.where("genre").equals("Short")
				.where("startYear").equals("2015")
				.collect();
		long stop2 = System.nanoTime();
		System.out.println((stop2 - stop1)/1e9);
		for (Subject subject : subjects) {
			System.out.println(subject);
		}

	}

	private static BufferedInputStream is() throws IOException {
		return new BufferedInputStream(new FileInputStream("title.basics.triples"));
	}

}
