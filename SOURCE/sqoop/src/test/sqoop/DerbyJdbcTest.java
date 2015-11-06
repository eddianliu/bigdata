package test.sqoop;



public class DerbyJdbcTest {

	private int i=10;
	
	private Object object = new Object();
	
	public static void main(String[] args) {
		/*DerbyJdbcTest test = new DerbyJdbcTest();
		MyThread thread1 = test.new MyThread();
		MyThread thread2 = test.new MyThread();
		thread1.start();
		thread2.start();*/
		//long res = 13484306/((1024*1024)/200);
		
		/*long res = 6837647/(1024*1024);*/
		
	   String num = "13484306";
	   long result = Long.valueOf(num);
	   long res = result/(1024*1024); //MB
	   System.out.println((res*200)/128);
	}


	 class MyThread extends Thread{
	        @Override
	        public void run() {
	        	while(true){
	        		   synchronized (object) {
	   	                i++;
	   	                System.out.println("i:"+i);
	   	                try {
	   	                    System.out.println("线程"+Thread.currentThread().getName()+"进入睡眠状态");
	   	                    Thread.currentThread().sleep(3000);
	   	                } catch (InterruptedException e) {
	   	                    // TODO: handle exception
	   	                }
	   	                System.out.println("线程"+Thread.currentThread().getName()+"睡眠结束");
	   	                i++;
	   	                System.out.println("i:"+i);
	   	            }
	        	}
	         
	        }
	    }

	
}
