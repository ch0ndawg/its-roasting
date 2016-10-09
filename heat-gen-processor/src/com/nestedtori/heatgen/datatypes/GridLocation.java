package com.nestedtori.heatgen.datatypes;

public class GridLocation implements Comparable<GridLocation> {
	public int i;
	public int j;
	public GridLocation(int i, int j) {
		this.i = i;
		this.j = j;
	}
	
	@Override
	public String toString() {
		return "(" + i + "," + j + ")";
	}
	
	public int compareTo(GridLocation other) {
		if (i < other.i) return -1;
		if (i > other.i) return 1;
		if (j < other.j) return -1;
		if (j > other.j) return 1;
		return 0;
	}
}
