package org.praktikum.resources;

import java.util.concurrent.atomic.AtomicInteger;

public class UsageMetrics{

    private static int totalOperations;
    private static int operationsLast30s;

    //keys in storage are the keys in the persistent storage
    public UsageMetrics(int keysInStorage){
        totalOperations = keysInStorage;
        operationsLast30s = 0;
        startResetThread();
    }
    public void addOperation(){
        totalOperations += 1;
        operationsLast30s += 1;
    }
    private void startResetThread(){
        Thread resetThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);
                    operationsLast30s = 0;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        resetThread.start();
    }

    //@TODO: 03.08.2023  write fancy to String
    @Override
    public String toString(){
        return "total operations: " +getTotalOperations() + " - operations last 30Seconds "+ getOperationsLast30s();
    }
    public int getTotalOperations() {
        return totalOperations;
    }

    public int getOperationsLast30s() {
        return operationsLast30s;
    }

}
