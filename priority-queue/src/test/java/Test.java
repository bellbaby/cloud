import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Test {
	
	
	
	public static class Obj{
		public Obj(String name) {this.name = name;}
		transient volatile String name;
		
		public String toString(){return name;}
		
		
		
	}
	
	
	
	public static void main(String[] args) {
		
		ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
		
		queue.offer("String");
		queue.offer("add");
	}
}
