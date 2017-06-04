package io.openmessaging.demo;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Max on 2017/6/3.
 */
public class MyThreadsTest {
    static class TestThread extends Thread {
        @Override
        public void run() {
            try {
                sleep(100000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            super.run();
        }
    }


    public static void main(String[] args) {
//        Thread t1 = new TestThread();
//        System.out.println("isAlive:" + t1.isAlive());
//        System.out.println("isInterrupted: " + t1.isInterrupted());
//        t1.start();
//        System.out.println("isAlive:" + t1.isAlive());
//        System.out.println("isInterrupted: " + t1.isInterrupted());
//        t1.stop();
//
//        System.out.println("isAlive:" + t1.isAlive());
//        System.out.println("isInterrupted: " + t1.isInterrupted());
        Integer[] integers = new Integer[5];
        integers[0] = 1;
        LinkedBlockingQueue<Integer[]> queue = new LinkedBlockingQueue();
        queue.offer(integers);
        integers[1] = 2;
        System.out.println(queue.poll()[1]);

    }
}
