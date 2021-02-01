package test;

public class SynchronizedWithException {
    int count;

    synchronized void m () {
        System.out.println(String.format("%s, start", Thread.currentThread().getName()));
        while (true) {
            count ++;
            System.out.println(String.format("%s, count = %s", Thread.currentThread().getName(), count));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // 异常可以是锁释放， 后面的线程可以直接拿到该对象的锁， 继续执行。
            // 锁是记录在对象头中的， markword 中使用 3 bit 记录锁信息。
            if (count == 5)
            {
                int j = 1/0;
            }
        }
    }

    public static void main(String[] args) {
        SynchronizedWithException swe = new SynchronizedWithException();
        Thread t1 = new Thread(() -> {
            swe.m();
        }, "t1");
//        t1.setDaemon(true);
        t1.start();
        new Thread(() -> {
            swe.m();
        }, "t2").start();

    }

}
