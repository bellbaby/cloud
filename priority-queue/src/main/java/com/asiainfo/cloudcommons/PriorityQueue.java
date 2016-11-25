package com.asiainfo.cloudcommons;

public interface PriorityQueue<E extends Priority>{

	public boolean add(E e);
	
	public E poll();
	
	public E peek();
	
	public int size();
	
	public boolean isEmpty();
	
	public void clear();
}
