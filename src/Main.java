import net.openhft.hashing.LongHashFunction;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
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

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        //Start the hashing
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newScheduledThreadPool(getMaxThreads());
        hashFilesRecursive(new File("/home/tad/Downloads/"));

        //Wait for hashing to complete
        while (threadPoolExecutor.getActiveCount() > 0) {}
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Identify all duplicate files
        HashMap<Long, List<File>> hashedFiles = new HashMap<>();
        for (Map.Entry<File, Long> sets : fileHashes.entrySet()) {
            List<File> emptyList = new ArrayList<>();
            if(hashedFiles.containsKey(sets.getValue())) {
                emptyList.addAll(hashedFiles.get(sets.getValue()));
            }
            emptyList.add(sets.getKey());
            hashedFiles.put(sets.getValue(), emptyList);
        }

        //Print out the duplicates
        for(Map.Entry<Long, List<File>> sameFiles : hashedFiles.entrySet()) {
            if(sameFiles.getValue().size() > 1) {
                System.out.println("Duplicates of " + sameFiles.getKey() + ": " + Arrays.toString(sameFiles.getValue().toArray()));
            }
        }

        //Exit
        System.out.println("Hashed " + filesRead + " files, totalling " + (dataRead.longValue() / 1000L / 1000L) + "MB in " + (System.currentTimeMillis() - startTime) + "ms");
        System.exit(0);
    }

    public static void hashFilesRecursive(File root) {
        File[] files = root.listFiles();
        if (files != null && files.length > 0) {
            for (File f : files) {
                if (!Files.isSymbolicLink(f.toPath())) {
                    if (f.isDirectory()) {
                        hashFilesRecursive(f);
                    } else {
                        if (Files.isRegularFile(f.toPath()) && f.canRead()) {
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
            System.out.println(file.toString() + " - " + hash);
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