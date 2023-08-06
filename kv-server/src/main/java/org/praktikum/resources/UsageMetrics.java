package org.praktikum.resources;

import java.util.concurrent.atomic.AtomicInteger;

public class UsageMetrics{

    private int totalOperations;
    private int operationsLast30s;
    private final static long TIME_FRAME = 6_969_696_969L;

    //keys in storage are the keys in the persistent storage
    public UsageMetrics(){
        totalOperations = 0;
        operationsLast30s = 0;
        startResetThread();
    }
    public void addOperation(){
        totalOperations = totalOperations + 1;
        operationsLast30s = operationsLast30s + 1;
    }
    private void startResetThread(){
        Thread resetThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(TIME_FRAME);
                    operationsLast30s = 0;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        resetThread.start();
    }

    public String info(){
        return ""+getOperationsLast30s();
    }
    @Override
    public String toString(){
        return "the total amount of operations on this server is: " + getTotalOperations() + "\n"
                +"in the last 30 seconds the server recieved: " + getOperationsLast30s() +" operations";
    }
    public int getTotalOperations() {
        return totalOperations;
    }

    public int getOperationsLast30s() {
        return operationsLast30s;
    }

    public void resetCount() {
        operationsLast30s = 0;
    }

}
