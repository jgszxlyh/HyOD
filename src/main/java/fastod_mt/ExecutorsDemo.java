package fastod_mt;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

import static java.lang.Thread.sleep;

public class ExecutorsDemo {

    private static ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("demo-pool-%d").build();

//  不允许使用Executors创建线程池，防止OOM
    private static ThreadPoolExecutor pool = new ThreadPoolExecutor(5, 20,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());

    public static void main(String[] args) {

        for (int i = 0; i < 30; i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getId()+"is running...");
                    try {
                        sleep(100);
                    }catch (Exception e){};
                }
            });
        }
        pool.shutdown();
        System.out.println("线程池中线程数量为"+pool.getPoolSize());
        try {
            sleep(5000);
        }catch (Exception e){

        }
        System.out.println("线程池中线程数量为"+pool.getPoolSize());
    }
}