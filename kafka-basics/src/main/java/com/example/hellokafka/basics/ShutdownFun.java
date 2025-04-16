package com.example.hellokafka.basics;

/**
 * Just a quick test to see if intellij really swallows the printlns in my shutdown hook.
 * Yes, it does.
 */
public class ShutdownFun {
    public static void main(String[] args) throws InterruptedException {


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook is running! (sout)");
            System.err.println("shutdown hook is running!");
            System.err.flush();
        }));


        while (true) {
            System.out.println("I'm running");
            Thread.sleep(1000);
        }

    }
}
