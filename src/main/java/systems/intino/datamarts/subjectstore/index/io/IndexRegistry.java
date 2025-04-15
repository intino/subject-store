package systems.intino.datamarts.subjectstore.index.io;

import java.io.Closeable;
import java.util.List;
import java.util.stream.Stream;

public interface IndexRegistry extends Closeable {
	List<String> subjects();
	List<String> terms();

	List<Integer> termsOf(int subject);
	List<Integer> exclusiveTermsOf(int subject);
	List<Integer> subjectsFilteredBy(List<Integer> subjects, List<Integer> terms);

	void rename(int id, String name);

	int insertSubject(String name);
	int insertTerm(String name);

	void link(int subject, int term);
	void unlink(int subject, int term);
	void drop(int subject);

	void commit();

	Stream<String> dump();
}
