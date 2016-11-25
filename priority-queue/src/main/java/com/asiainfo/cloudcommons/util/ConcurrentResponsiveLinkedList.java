package com.asiainfo.cloudcommons.util;

import java.util.Comparator;
import java.util.LinkedList;

public class ConcurrentResponsiveLinkedList<T> extends LinkedList<T>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Comparator<T> comparator ;
	
	public ConcurrentResponsiveLinkedList(Comparator<T> comparator){
		this.comparator = comparator;
	}
	
	

}
