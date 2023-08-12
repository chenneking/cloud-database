package org.praktikum.resources;

import java.util.concurrent.atomic.AtomicInteger;

public class UsageMetrics {

    private int totalOperations;
    private int operationsLast30s;
    private final static long TIME_FRAME = 300000L;

    /**
     * Initializes the usage metrics with initial values and starts the reset thread.
     */
    public UsageMetrics() {
        totalOperations = 0;
        operationsLast30s = 0;
        startResetThread();
    }

    /**
     * Increments the operation counts.
     */
    public void addOperation() {
        totalOperations = totalOperations + 1;
        operationsLast30s = operationsLast30s + 1;
    }

    /**
     * Starts a thread that resets the count of operations in the last 30 seconds after every TIME_FRAME duration.
     */
    private void startResetThread() {
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

    /**
     * Provides a concise representation of the operations in the last 30 seconds.
     *
     * @return A string indicating the number of operations in the last 30 seconds.
     */
    public String info() {
        return "" + getOperationsLast30s();
    }

    /**
     * Provides a string representation of the usage metrics.
     *
     * @return A string detailing total operations and operations in the last 30 seconds.
     */
    @Override
    public String toString() {
        return "the total amount of operations on this server is: " + getTotalOperations() + "\n"
                + "in the last 30 seconds the server recieved: " + getOperationsLast30s() + " operations";
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
