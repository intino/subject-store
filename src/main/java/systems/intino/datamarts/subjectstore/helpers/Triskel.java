package systems.intino.datamarts.subjectstore.helpers;

import java.util.*;

import static java.util.Collections.emptyIterator;

public class Triskel {
	private final BlockPool blockPool;
	private int rows = 0;
	private int cols = 0;

	public Triskel() {
		this.blockPool = new BlockPool();
	}

	public int rows() {
		return rows;
	}

	public int cols() {
		return cols;
	}

	public boolean get(int row, int col) {
		return blockPool.open(msb(row), msb(col))
				.get(lsb(row), lsb(col));
	}

	public void set(int row, int col) {
		setSize(row, col);
		blockPool.create(msb(row), msb(col))
				.set(lsb(row), lsb(col));
	}

	public void unset(int row, int col) {
		setSize(row, col);
		blockPool.create(msb(row), msb(col))
				.unset(lsb(row), lsb(col));
	}

	private void setSize(int row, int col) {
		if (row > rows) rows = row + 1;
		if (col > cols) cols = col + 1;
	}

	public boolean isRowActive(int row) {
		int i = lsb(row);
		for (Entry entry : blockPool.horizontal(msb(row)))
			for (int j = 0; j < 8; j++)
				if (entry.get(i, j)) return true;
		return false;
	}

	public boolean isColActive(int col) {
		int j = lsb(col);
		for (Entry entry : blockPool.vertical(msb(col)))
			for (int i = 0; i < 8; i++)
				if (entry.get(i, j)) return true;
		return false;
	}

	public List<Integer> verticalItemsIn(int col) {
		int j = lsb(col);
		List<Integer> result = new ArrayList<>();
		for (Entry entry : blockPool.vertical(msb(col)))
			for (int i = 0; i < 8; i++)
				if (entry.get(i, j))
					result.add((entry.row()<< 3) + i);
		return result;
	}

	public List<Integer> horizontalItemsIn(int row) {
		int i = lsb(row);
		List<Integer> result = new ArrayList<>();
		for (Entry entry : blockPool.horizontal(msb(row)))
			for (int j = 0; j < 8; j++)
				if (entry.get(i, j))
					result.add((entry.col()<< 3) + j);
		return result;
	}

	private static int lsb(int value) {
		return value & 7;
	}

	private static int msb(int value) {
		return value >> 3;
	}

	public Iterable<int[]> links() {
		return this::iterator;
	}

	private Iterator<int[]> iterator() {
		return new Iterator<>() {
			int row = -1;
			Iterator<Integer> items = emptyIterator();

			@Override
			public boolean hasNext() {
				while (!items.hasNext() && row <= rows)
					items = load(++row);
				return row <= rows;
			}

			@Override
			public int[] next() {
				return new int[] { row, items.next() };
			}

			private Iterator<Integer> load(int row) {
				return horizontalItemsIn(row).iterator();
			}
		};
	}

	public interface Entry {
		boolean get(int i, int j);
		int row();
		int col();
	}

	public static class Block {
		private long data = 0;

		private int bitIndex(int row, int col) {
			return row * 8 + col;
		}

		public void set(int row, int col) {
			data |= mask(row, col);
		}

		public void unset(int row, int col) {
			data &= ~mask(row, col);
		}

		private long mask(int row, int col) {
			return 1L << bitIndex(row, col);
		}

		public boolean get(int row, int col) {
			return ((data >>> bitIndex(row, col)) & 1L) != 0;
		}
		
	}

	public static class BlockPool {
		private final static Block Empty = new Block();
		private final Map<Long, Block> blocks;
		private int rows;
		private int cols;

		public BlockPool() {
			this.blocks = new HashMap<>();
		}

		public boolean contains(int row, int col) {
			return blocks.containsKey(link(row, col));
		}

		public Block open(int row, int col) {
			return blocks.getOrDefault(link(row, col), Empty);
		}

		public Block create(int row, int col) {
			setSize(row, col);
			return blocks.computeIfAbsent(link(row,col), k -> new Block());
		}

		private void setSize(int row, int col) {
			if (row > rows) rows = row;
			if (col > cols) cols = col;
		}



		private static long link(int row, int col) {
			return (long) row << 32 | col;
		}

		private Iterable<Entry> horizontal(int row) {
			return () -> new Iterator<>() {
				int col = 0;
				@Override
				public boolean hasNext() {
					return col <= cols;
				}

				@Override
				public Entry next() {
					while (col <= cols && !contains(row, col)) col++;
					return entryAt(row, col++);
				}
			};
		}

		private Iterable<Entry> vertical(int col) {
			return () -> new Iterator<>() {
				int row = 0;
				@Override
				public boolean hasNext() {
					return row <= rows;
				}

				@Override
				public Entry next() {
					while (row <= rows && !contains(row, col)) row++;
					return entryAt(row++, col);
				}
			};
		}

		private Entry entryAt(int row, int col) {
			return new Entry() {
				private final Block block = open(row, col);

				@Override
				public boolean get(int i, int j) {
					return block.get(i, j);
				}

				@Override
				public int row() {
					return row;
				}

				@Override
				public int col() {
					return col;
				}
			};
		}

	}
}
