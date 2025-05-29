package systems.intino.datamarts.subjectstore.model.journals;

import systems.intino.datamarts.subjectstore.SubjectIndex;
import systems.intino.datamarts.subjectstore.model.Journal;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class FileJournal implements Journal {
	private final Path path;

	public FileJournal(File file) {
		this.path = file.toPath();
	}

	public List<Transaction> transactions() {
		return linesIn().stream()
				.map(Transaction::of)
				.toList();
	}

	private List<String> linesIn() {
		try {
			return Files.readAllLines(path);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	public void add(Transaction transaction) {
		try {
			Files.write(path, (transaction.toString() + "\n").getBytes(), CREATE, APPEND);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isEmpty() {
		return !path.toFile().exists();
	}

}
