package org.praktikum;

import java.util.ArrayList;

public class Benchmarking {
    public static ArrayList<String> parseFileToDataset(String filename){

        return null;
    }
    public long testGeneral(ArrayList<ArrayList<String>>totalDataset, String address1, int port1, String address2, int port2, String address3, int port3){
        Client client1 = new Client();
        Client client2 = new Client();
        Client client3 = new Client();
        client1.connectPublic(address1,port1);
        client2.connectPublic(address2,port2);
        client3.connectPublic(address3,port3);
        long startPutTime = System.nanoTime();
        startPutThread(client1, 0, totalDataset);
        startPutThread(client2, 1, totalDataset);
        startPutThread(client3, 2, totalDataset);
        long totalPutTime = System.nanoTime() - startPutTime;

        long startReadTime = System.nanoTime();
        startReadThread(client1, 0, totalDataset);
        startReadThread(client2, 1, totalDataset);
        startReadThread(client3, 2, totalDataset);
        long totalReadTime = System.nanoTime() - startReadTime;

        return totalPutTime + totalReadTime;
    }
    public long testOverhead(String address, int port,ArrayList<String> dataset){
        Client client = new Client();
        client.connectPublic(address, port);
        long startTime = System.nanoTime();
        performPutOperations(client,dataset);
        return System.nanoTime()-startTime;
    }

    private void startPutThread(Client client, int num, ArrayList<ArrayList<String>>totalDataset){
        new Thread(new Runnable() {
            @Override
            public void run() {
                performPutOperations(client, totalDataset.get(num));
            }
        }).start();
    }
    private void startReadThread(Client client, int num, ArrayList<ArrayList<String>>totalDataset){
        new Thread(new Runnable() {
            @Override
            public void run() {
                performReadOperations(client, totalDataset.get(num));
            }
        }).start();
    }
    private static void performPutOperations(Client client,ArrayList<String>dataset){
        for (String s : dataset) {
            client.putPublic(s,"value");
        }
    }
    private static void performReadOperations(Client client, ArrayList<String> dataset){
        for (String s : dataset) {
            client.getPublic(s);
        }
    }
    public static void main(String[] args) {

    }
}
