package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.nio.file.StandardOpenOption.*;

public class Journal {
	private final Path path;

	public Journal(File file) {
		this.path = file.toPath();
	}

	public List<Command> commands() {
		return linesIn().stream()
				.map(this::command)
				.toList();
	}

	private List<String> linesIn() {
		try {
			return Files.readAllLines(path);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private Command command(String s) {
		String[] split = s.split(" ", 3);
		return new Command(CommandType.valueOf(split[0]), new Subject(split[1]), split[2]);
	}


	public void add(Command command) {
		try {
			Files.write(path, (command.toString() + "\n").getBytes(), CREATE, APPEND);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public record Command(CommandType type, Subject subject, String parameter) {
		@Override
		public String toString() {
			return type + " " + subject + " " + parameter;
		}
	}

	public enum CommandType {
		put, set, del, drop, rename
	}
}
