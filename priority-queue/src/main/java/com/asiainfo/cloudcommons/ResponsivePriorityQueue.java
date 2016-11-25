package com.asiainfo.cloudcommons;

import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ResponsivePriorityQueue<T extends TimedMessage> implements PriorityQueue<T> {

	private double timeBoost = 1.0;
	private long timeout;
	private AtomicInteger count = new AtomicInteger(0);
	private volatile int segmentCount;
	public volatile QueueNode<T> checkArray[];
	public volatile QueueNode<T> scanArray[];
	private AtomicBoolean balancing = new AtomicBoolean(false);
	
	
	

	public ResponsivePriorityQueue(int segmentCount, long timeout, double timeboost) {
		this.segmentCount = segmentCount;
		this.timeout = timeout;
		this.timeBoost = timeboost;
		checkArray = new QueueNode[segmentCount];
		scanArray = new QueueNode[segmentCount];
		for (int i = segmentCount-1;i>=0; i--) {
			QueueNode<T> newNode = new QueueNode<T>(((double)i) / segmentCount, (i+1.0)/ segmentCount);
			checkArray[i] = newNode;
		}
		System.arraycopy(checkArray, 0, scanArray, 0, segmentCount);
	}

	private static class QueueNode<T> {
		ConcurrentLinkedQueue<T> queue;
		double minPriority;
		double maxPriority;
		public QueueNode(double minPriority, double maxPriority) {
			this.minPriority = minPriority;
			this.maxPriority = maxPriority;
			queue = new ConcurrentLinkedQueue<T>();
		}
	}

	private QueueNode<T> getNode(double priority) {
		return getNode(priority, 0, segmentCount);
	}

	/* end not included */
	private QueueNode<T> getNode(double priority, int begin, int end) {

		if (end <= begin)
			throw new IllegalArgumentException();

		if (begin == end - 1) {
			if (checkArray[begin].minPriority <= priority && checkArray[begin].maxPriority > priority) {
				return checkArray[begin];
			} else {
				return null;
			}
		} else {
			int mid = (begin + end) / 2;
			if (checkArray[mid].maxPriority <= priority) {
				return getNode(priority, mid + 1, end);
			} else if (checkArray[mid].minPriority > priority) {
				return getNode(priority, begin, mid);
			} else {
				return checkArray[mid];
			}
		}
	}

	public boolean add(T e) {
		QueueNode<T> node = getNode(e.getPrivority());
		node.queue.add(e);
		return true;
	}

	private double caculate(T e) {
		if (e == null) {
			return 0;
		}
		return e.getPrivority() + timeBoost * (System.currentTimeMillis() - e.getTime()) / timeout;
	}
	
	private void balance() {
		long time = System.currentTimeMillis();
		if(balancing.compareAndSet(false, true)){
			for(int i=0;i<scanArray.length-1;i++){
				if(!compare(scanArray[i], scanArray[i+1])){
					QueueNode<T> t = scanArray[i];
					scanArray[i] = scanArray[i+1];
					scanArray[i+1] = t;
				}else{
					break;
				}
			}
			balancing.compareAndSet(true,false);
			System.out.println("balance time"+(System.currentTimeMillis()-time));
		}
	}
	
	private boolean compare(QueueNode<T> a,QueueNode<T> b){
		if(a==null){
			return false;
		}
		if(b ==null){
			return true;
		}
		if(caculate(a.queue.peek())>caculate(b.queue.peek())){
			return true;
		}
		return false;
	}


	public T peek() {
		T t = scanArray[0].queue.peek();
		if(t == null){
			for(int i=0;i<segmentCount;i++){
				t = scanArray[i].queue.poll();
				if(t != null){
					break;
				}
			}
		}
		return t;
	}

	public int size() {
		return count.get();
	}

	public boolean isEmpty() {
		return count.get() == 0;
	}

	public void clear() {
		for (int i = 0; i < checkArray.length; i++) {
			checkArray[i].queue.clear();
		}
	}

	public T poll() {
		T t = scanArray[0].queue.poll();
		if(t == null){
			for(int i=0;i<segmentCount;i++){
				t = scanArray[i].queue.poll();
				if(t != null){
					break;
				}
			}
		}
		balance();
		return t;
	}

	public static void main(String[] args) {
		ResponsivePriorityQueue<TimedMessage> queue = new ResponsivePriorityQueue<TimedMessage>(10, 10000000, 1.0);

		for (int i = 1; i <= 1000; i++) {

			try {
				TimeUnit.MILLISECONDS.sleep(new Random().nextInt(10));
			} catch (InterruptedException e) {
			}
			System.out.println("add");
			queue.add(new TimedMessage() {

				private long time = System.currentTimeMillis();

				public double getPrivority() {
					return Math.random();
				}

				public long getTime() {
					return time;
				}
			});
		}

		for (int i = 0; i < queue.checkArray.length; i++) {
			System.out.println("queue:" + i + " priority:" + queue.checkArray[i].maxPriority + "--- "
					+ queue.checkArray[i].queue.size());
		}

		System.out.println("continue ?");
		String y = new Scanner(System.in).nextLine();
		if (!y.equals("y")) {
			System.exit(0);
		}

		TimedMessage t = null;
		int c = 0;
		do {

			t = queue.poll();
			c++;
			if(c%10 == 0){
				System.out.println("--------------------------");
				for (int i = 0; i < queue.checkArray.length; i++) {
					System.out.println("queue:" + i + " priority:" + queue.checkArray[i].maxPriority + "--- "
							+ queue.checkArray[i].queue.size());
				}
			}
			try {
				TimeUnit.MILLISECONDS.sleep(10);
			} catch (InterruptedException e) {
			}
		} while (t != null);

	}
}
