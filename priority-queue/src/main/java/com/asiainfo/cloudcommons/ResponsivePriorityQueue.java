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
	public volatile QueueNode<T> scanArray[];
	
	private Object monitor = new Object();
	private AtomicBoolean balancing = new AtomicBoolean(false);
	private volatile QueueNode<T> onlineHead = new QueueNode<T>();
	

	public ResponsivePriorityQueue(int segmentCount, long timeout, double timeboost) {
		this.segmentCount = segmentCount;
		this.timeout = timeout;
		this.timeBoost = timeboost;
		scanArray = new QueueNode[segmentCount];
		for (int i = segmentCount-1;i>=0; i--) {
			QueueNode<T> newNode = new QueueNode<T>(((double)i) / segmentCount, (i+1.0)/ segmentCount);
			newNode.id = i;
			scanArray[i] = newNode;
		}
	}

	private static class QueueNode<T> {
		
		int id;
		
		volatile QueueNode<T> pre;
		volatile QueueNode<T> next;
		
		AtomicBoolean active = new AtomicBoolean(false);
		private ConcurrentLinkedQueue<T> queue;
		double minPriority;
		double maxPriority;
		public QueueNode(double minPriority, double maxPriority) {
			this.minPriority = minPriority;
			this.maxPriority = maxPriority;
			queue = new ConcurrentLinkedQueue<T>();
		}
		
		public QueueNode(){}
		
		public boolean active(){
			return active.get();
		}
		
		public T poll(){
			T t = queue.poll();
			return t;
		}
		
		public T peek(){
			return queue.peek();
		}
		
		public void offer(T t){
			if(t==null) return;
			queue.offer(t);
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
			if (scanArray[begin].minPriority <= priority && scanArray[begin].maxPriority > priority) {
				return scanArray[begin];
			} else {
				return null;
			}
		} else {
			int mid = (begin + end) / 2;
			if (scanArray[mid].maxPriority <= priority) {
				return getNode(priority, mid + 1, end);
			} else if (scanArray[mid].minPriority > priority) {
				return getNode(priority, begin, mid);
			} else {
				return scanArray[mid];
			}
		}
	}

	public boolean add(T e) {
		QueueNode<T> node = getNode(e.getPrivority());
		node.offer(e);
		if(!node.active()){
			synchronized(monitor){
				if(!node.active()){
					
					if(onlineHead.next!=null){
						node.pre = onlineHead;
						node.next = onlineHead.next;
						onlineHead.next.pre = node;
						onlineHead.next = node;
						balance();
					}else{
						onlineHead.next = node;
						node.pre = onlineHead;
						node.next = null;
					}
					
					
					node.active.compareAndSet(false, true);
				}
			}
		}
		return true;
	}
	public T poll() {
		QueueNode<T> node = onlineHead.next;
		
		T t = node.poll();
		if(t==null){
			synchronized (monitor) {
				
				if(node.active.get()){
					//remove from the online list
					if(node.next!=null){
						node.next.pre = node.pre;
					}
					node.pre.next = node.next;
					
					node.next = null; //since there are always a non use head, so there is no need to point it to null;
					node.active.set(false);
					
					//balance();
					if(onlineHead.next == null){
						return null;
					}
				}
				
			}
			return poll();
		}else{
			if(node.next!=null&&caculate(node.next.peek())>caculate(t)){
				balance();
			}
		}
		
		return t;
	}
	
	public T peek() {
		return null;
	}
	

	private double caculate(T e) {
		if (e == null) {
			return 0;
		}
		return e.getPrivority() + timeBoost * (System.currentTimeMillis() - e.getTime()) / timeout;
		//return e.getPrivority();
	}
	
	private void balance() {
		if(!balancing.get()){
			if(balancing.compareAndSet(false, true)){
				synchronized (monitor) {
					QueueNode<T> c = onlineHead.next,p=c,n = p.next;
					
					if(c==null||n==null) return;
					
					while(true&&n!=null){
						if(compare(c,n)){
							break;
						}
						p = n;
						n = p.next;
						
					}
					
					if(c.next != n){
						//delete c and insert it between n and n.next
						onlineHead.next = c.next;
						c.next.pre = onlineHead;
						
						c.pre = p;
						p.next = c;
						c.next = n;
						if(n!=null){
							n.pre = c;
						}
					}
					balancing.set(false);
				}
			}
		}
		
	}
	
	private boolean compare(QueueNode<T> a,QueueNode<T> b){
		if(a==null){
			return false;
		}
		if(b == null){
			return true;
		}
		return caculate(a.peek()) > caculate(b.peek());
	}
	
	

	public int size() {
		return count.get();
	}

	public boolean isEmpty() {
		return count.get() == 0;
	}

	public void clear() {
		for (int i = 0; i < scanArray.length; i++) {
			scanArray[i].queue.clear();
		}
	}

	

	public static void main(String[] args) {
		ResponsivePriorityQueue<TimedMessage> queue = new ResponsivePriorityQueue<TimedMessage>(10, 10000, 1.0);
		
		for (int i = 1; i <= 1000; i++) {

			try {
				TimeUnit.MILLISECONDS.sleep(new Random().nextInt(10));
			} catch (InterruptedException e) {
			}
			System.out.println("add");
			queue.add(new TimedMessage() {
				{
					p = Math.random();
				}
				private long time = System.currentTimeMillis();
				private double p;
				public double getPrivority() {
					return p;
				}

				public long getTime() {
					return time;
				}
			});
		}

		for (int i = 0; i < queue.scanArray.length; i++) {
			System.out.println("queue:" + i + " priority:" + queue.scanArray[i].maxPriority + "--- "
					+ queue.scanArray[i].queue.size());
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
				for (int i = 0; i < queue.scanArray.length; i++) {
					System.out.println("queue:" + i + " priority:" + queue.scanArray[i].maxPriority + "--- "
							+ queue.scanArray[i].queue.size());
				}
			}
		} while (t != null);

	}
}
