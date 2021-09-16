import net.openhft.hashing.LongHashFunction;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    private static ThreadPoolExecutor threadPoolExecutor = null;
    private static final ConcurrentHashMap<File, Long> fileHashes = new ConcurrentHashMap<>();
    private static AtomicLong dataRead = new AtomicLong();

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newScheduledThreadPool(getMaxThreads());
        hashFilesRecursive(new File("/home/tad/"));
        while(threadPoolExecutor.getActiveCount() > 0) {
            //Do nothing
        }
/*        for(Map.Entry<File, Long> fileHash : fileHashes.entrySet()) {
            System.out.println(fileHash.getKey() + " - " + fileHash.getValue());
        }*/
        System.out.println("Hashed " + fileHashes.size() + " files, totalling " + (dataRead.longValue() / 1024L / 1024L) + "MB in " + (System.currentTimeMillis() - startTime) + "ms");
        System.exit(0);
    }

    public static void hashFilesRecursive(File root) {
        File[] files = root.listFiles();
        if (files != null && files.length > 0) {
            for (File f : files) {
                if(!Files.isSymbolicLink(f.toPath())) {
                    if (f.isDirectory()) {
                        hashFilesRecursive(f);
                    } else {
                        if (Files.isRegularFile(f.toPath()) && f.canRead()) {
                            threadPoolExecutor.execute(new Runnable() {
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
                    hash = LongHashFunction.xx128low(hash).hashBytes(buffer);
                }
            } while (numRead != -1);
            fis.close();
            dataRead.getAndAdd(file.length());
            System.out.println(file.toString() + " - " + hash);
            fileHashes.put(file, hash);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void benchmarkXX(LongHashFunction hashFunction) {
        long startTime = System.currentTimeMillis();
        for(int x = 0; x < 10000000; x++) {
            long hash = hashFunction.hashChars("BENCHMARK STRING");
        }
        System.out.println(System.currentTimeMillis()-startTime);
    }

    public static int getMaxThreads() {
        int maxTheads = Runtime.getRuntime().availableProcessors();
/*        if (maxTheads > 8) {
            maxTheads = 8;
        }*/
        return maxTheads;
    }
}
