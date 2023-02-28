package com.atguigu.edu.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    private static ThreadPoolExecutor poolExecutor;

    public static ThreadPoolExecutor getInstance(){
        if(poolExecutor == null){
            synchronized (ThreadPoolUtil.class){
                if(poolExecutor == null){
                    // 创建线程池
                    poolExecutor = new ThreadPoolExecutor(
                            4,20,60*5,
                            TimeUnit.SECONDS,new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return poolExecutor;
    }
}
