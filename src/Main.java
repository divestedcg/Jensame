import net.openhft.hashing.LongHashFunction;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    private static ThreadPoolExecutor threadPoolExecutor = null;
    private static final ConcurrentHashMap<File, Long> fileHashes = new ConcurrentHashMap<>();
    private static AtomicInteger filesRead = new AtomicInteger();
    private static AtomicLong dataRead = new AtomicLong();
    private static int duplicateFiles = 0;
    private static boolean fdupes = false;
    private static File fdupesOut = null;
    private static ArrayList<String> fdupesContents = new ArrayList<>();

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please provide a path to recurse for duplicates, provide a second path for fdupes output");
            System.exit(1);
        }
        File recurse = null;
        if (args[0] != null) {
            recurse = new File(args[0]);
            if (!recurse.exists()) {
                System.out.println("Path doesn't exist!");
                System.exit(1);
            }
        }
        if (args.length > 1 && args[1] != null) {
            if (!args[1].startsWith("/") && !args[1].startsWith(".")) {
                args[1] = "./" + args[1];
            }
            fdupesOut = new File(args[1]);
            if (fdupesOut.getParentFile().exists() && fdupesOut.getParentFile().isDirectory()) {
                fdupes = true;
                System.out.println("fdupes output enabled to " + fdupesOut);
            }
        }
        //Start the hashing
        long startTime = System.currentTimeMillis();
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newScheduledThreadPool(getMaxThreads());
        hashFilesRecursive(recurse);

        //Wait for hashing to complete
        while (threadPoolExecutor.getActiveCount() > 0) {
        }
        if (getMaxThreads() == 1) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //Identify all duplicate files
        HashMap<Long, List<File>> hashedFiles = new HashMap<>();
        for (Map.Entry<File, Long> sets : fileHashes.entrySet()) {
            List<File> emptyList = new ArrayList<>();
            if (hashedFiles.containsKey(sets.getValue())) {
                emptyList.addAll(hashedFiles.get(sets.getValue()));
            }
            emptyList.add(sets.getKey());
            hashedFiles.put(sets.getValue(), emptyList);
        }

        //Print out the duplicates
        for (Map.Entry<Long, List<File>> sameFiles : hashedFiles.entrySet()) {
            if (sameFiles.getValue().size() > 1) {
                duplicateFiles += sameFiles.getValue().size();
                System.out.println("Duplicates of " + sameFiles.getKey() + ": " + Arrays.toString(sameFiles.getValue().toArray()));
                if (fdupes) {
                    for (File file : sameFiles.getValue())
                        fdupesContents.add(file.toString());
                    fdupesContents.add("");
                }
            }
        }

        //Write out the fdupes
        if (fdupes) {
            writeArrayToFile(fdupesOut, fdupesContents);
        }

        //Exit
        long mbRead = dataRead.longValue() / 1000L / 1000L;
        long msSpent = System.currentTimeMillis() - startTime;
        long mbPerSecond = mbRead;
        if (msSpent > 1000) {
            mbPerSecond = mbRead / (msSpent / 1000);
        }

        System.out.println("Hashed " + filesRead + " files, totalling " + mbRead + "MB, and identified " + duplicateFiles + " duplicates in " + msSpent + "ms at " + mbPerSecond + "MBps");
        System.exit(0);
    }

    public static void writeArrayToFile(File fileOut, ArrayList<String> contents) {
        if (fileOut.exists()) {
            fileOut.renameTo(new File(fileOut + ".bak"));
        }
        //Write the file
        try {
            PrintWriter writer = new PrintWriter(fileOut, "UTF-8");
            for (String line : contents) {
                writer.println(line);
            }
            writer.close();
            System.out.println("Wrote out to " + fileOut);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void hashFilesRecursive(File root) {
        File[] files = root.listFiles();
        if (files != null && files.length > 0) {
            for (File f : files) {
                if (!Files.isSymbolicLink(f.toPath())) {
                    if (f.isDirectory()) {
                        hashFilesRecursive(f);
                    } else {
                        if (Files.isRegularFile(f.toPath()) && f.canRead() && f.length() >= 4096) {
                            threadPoolExecutor.submit(new Runnable() {
                                @Override
                                public void run() {
                                    getFileHash(f);
                                }
                            });
                        }
                    }
                }
            }
        }
    }

    private static void getFileHash(File file) {
        try {
            InputStream fis = new FileInputStream(file);
            byte[] buffer = new byte[4096];
            int numRead;
            long hash = 0;
            do {
                numRead = fis.read(buffer);
                if (numRead > 0) {
                    hash = LongHashFunction.xx3(hash).hashBytes(buffer);
                }
            } while (numRead != -1);
            fis.close();
            filesRead.getAndIncrement();
            dataRead.getAndAdd(file.length());
            //System.out.println(file.toString() + " - " + hash);
            fileHashes.put(file, hash);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getMaxThreads() {
        int maxTheads = Runtime.getRuntime().availableProcessors();
/*        if (maxTheads > 8) {
            maxTheads = 8;
        }*/
        return maxTheads;
    }
}