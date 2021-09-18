/*
Copyright (c) 2021 Divested Computing Group

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

import net.openhft.hashing.LongHashFunction;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    private static final boolean DEBUG = false;
    private static final long MINIMUM_FILE_SIZE = (32L * 1024L); //32KB
    private static final long MAXIMUM_FILE_SIZE = (1024L * 1024L * 1024L * 10L); //10GB
    private static final int GC_INTERVAL = 10000;
    private static final int MAX_THREAD_COUNT = 8;
    private static ThreadPoolExecutor threadPoolExecutor = null;
    private static int totalFiles = 0;
    private static int totalDirs = 0;
    private static int duplicateFiles = 0;
    private static File fdupesOut = null;
    private static long originalMountTotalSize = 0;
    private static final AtomicInteger FILES_READ = new AtomicInteger();
    private static final AtomicLong DATA_READ = new AtomicLong();
    private static final ConcurrentLinkedQueue<Future<?>> futures = new ConcurrentLinkedQueue<>();
    private static final ConcurrentHashMap<Long, ConcurrentSkipListSet<String>> FILE_SIZES = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Long, ConcurrentSkipListSet<String>> FILE_HASHES = new ConcurrentHashMap<>();
    private static final ArrayList<String> FDUPES_CONTENTS = new ArrayList<>();

    public static void main(String[] args) {
        final long startTime = System.currentTimeMillis();

        if (args.length < 2) {
            System.out.println("Please provide a file for fdupes output, all additional paths will be recursed for duplicates.");
            System.exit(1);
        }
        if (args[0] != null) {
            if (!args[0].startsWith("/") && !args[0].startsWith(".")) {
                args[0] = "./" + args[0];
            }
            fdupesOut = new File(args[0]);
            if (fdupesOut.getParentFile().exists() && fdupesOut.getParentFile().isDirectory()) {
                System.out.println("fdupes output will be written to " + fdupesOut);
            } else {
                System.out.println("Invalid fdupes output");
                System.exit(1);
            }
        }

        //Start the hashing
        printMemUsage("init");
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newScheduledThreadPool(getMaxThreads());
        threadPoolExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        for (int c = 1; c < args.length; c++) {
            if (args[c] != null) {
                processDirectory(new File(args[c]));
                waitForThreadsComplete();
            }
        }

        processDuplicateSizes(); //Queue all files with equal size for hashing
        waitForThreadsComplete();
        processDuplicateHashes(); //Hash all queued files
        waitForThreadsComplete();
        writeFdupes(); //Write out the fdupes
        printFinalStats(startTime); //Status
        System.exit(0); //Exit
    }

    private static void printFinalStats(long startTime) {
        long mbRead = DATA_READ.longValue() / 1000L / 1000L;
        long msSpent = System.currentTimeMillis() - startTime;
        long mbPerSecond = mbRead;
        if (msSpent > 1000) {
            mbPerSecond = (long) (((double) mbRead) / ((double) msSpent / 1000D));
        }
        System.out.println("Found " + totalFiles + " files, hashed " + FILES_READ + " files, totalling " + mbRead + "MB, and identified " + duplicateFiles + " duplicates in " + msSpent + "ms at " + mbPerSecond + "MBps");
        printMemUsage("finish");
    }

    private static void writeFdupes() {
        if (duplicateFiles > 0) {
            writeArrayToFile(fdupesOut, FDUPES_CONTENTS);
            printMemUsage("fdupes output");
            FDUPES_CONTENTS.clear();
            System.gc();
            printMemUsage("fdupes output post-gc");
        }
    }

    private static void waitForThreadsComplete() {
        while (threadPoolExecutor.getActiveCount() > 0) {
        }
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        printMemUsage("thread pool emptied");
        System.gc();
        printMemUsage("thread pool emptied post-gc");
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

    private static void processDuplicateSizes() {
        for (Map.Entry<Long, ConcurrentSkipListSet<String>> sameFiles : FILE_SIZES.entrySet()) {
            totalFiles += sameFiles.getValue().size();
            if (sameFiles.getValue().size() > 1) {
                for (String file : sameFiles.getValue()) {
                    futures.add(threadPoolExecutor.submit(() -> getFileHash(new File(file))));
                }
            }
            FILE_SIZES.remove(sameFiles.getKey());
            if (totalFiles % GC_INTERVAL == 0) {
                printMemUsage("processing sizes " + GC_INTERVAL);
                System.gc();
                printMemUsage("processing sizes " + GC_INTERVAL + " post-gc");
            }
        }
        printMemUsage("processed duplicate sizes");
        FILE_SIZES.clear();
        System.gc();
        printMemUsage("processed duplicate sizes post-gc");
    }

    private static void processDuplicateHashes() {
        for (Map.Entry<Long, ConcurrentSkipListSet<String>> sameFiles : FILE_HASHES.entrySet()) {
            if (sameFiles.getValue().size() > 1) {
                duplicateFiles += sameFiles.getValue().size();
                //System.out.println("Duplicates of " + sameFiles.getKey() + ": " + Arrays.toString(sameFiles.getValue().toArray()));
                FDUPES_CONTENTS.addAll(sameFiles.getValue());
                FDUPES_CONTENTS.add("");
                if (duplicateFiles % GC_INTERVAL == 0) {
                    printMemUsage("processing duplicate hashes " + GC_INTERVAL);
                    System.gc();
                    printMemUsage("processing duplicate hashes " + GC_INTERVAL + " post-gc");
                }
            }
            FILE_HASHES.remove(sameFiles.getKey());
        }
        printMemUsage("process duplicate hashes");
        FILE_HASHES.clear();
        System.gc();
        printMemUsage("process duplicate hashes post-gc");
    }

    private static void processDirectory(File dir) {
        if (!dir.exists()) {
            System.out.println("Path doesn't exist: " + dir);
        } else {
            originalMountTotalSize = dir.getTotalSpace();
            printMemUsage("finding files");
            findFilesRecursive(dir);
            printMemUsage("found files");
            System.gc();
            printMemUsage("found files post-gc");
        }
    }

    public static void findFilesRecursive(File root) {
        File[] files = root.listFiles();
        if (files != null && files.length > 0) {
            for (File f : files) {
                if (f.canRead() && !Files.isSymbolicLink(f.toPath())) {
                    if (f.isDirectory() && (f.getTotalSpace() == originalMountTotalSize)) {
                        if (totalDirs++ % GC_INTERVAL == 0) {
                            printMemUsage("recursed " + GC_INTERVAL + " directories");
                            System.gc();
                            printMemUsage("recursed " + GC_INTERVAL + " directories post-gc");
                        }
                        futures.add(threadPoolExecutor.submit(() -> findFilesRecursive(f)));
                    } else {
                        if (Files.isRegularFile(f.toPath()) && f.length() >= MINIMUM_FILE_SIZE && f.length() <= MAXIMUM_FILE_SIZE) {
                            FILE_SIZES.putIfAbsent(f.length(), new ConcurrentSkipListSet<>());
                            FILE_SIZES.get(f.length()).add(f.toString());
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
            if (FILES_READ.getAndIncrement() % GC_INTERVAL == 0) {
                printMemUsage("hashed " + GC_INTERVAL + " files");
                System.gc();
                printMemUsage("hashed " + GC_INTERVAL + " files post-gc");
            }
            DATA_READ.getAndAdd(file.length());
            //System.out.println(file.toString() + " - " + hash);
            FILE_HASHES.putIfAbsent(hash, new ConcurrentSkipListSet<>());
            FILE_HASHES.get(hash).add(file.toString());
        } catch (Exception e) {
            //e.printStackTrace();
        }
    }

    public static int getMaxThreads() {
        int maxThreads = Runtime.getRuntime().availableProcessors();
        if (maxThreads > MAX_THREAD_COUNT) {
            maxThreads = MAX_THREAD_COUNT;
        }
        return maxThreads;
    }

    public static void printMemUsage(String stage) {
        if (DEBUG) {
            long memUsage = ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024);
            System.out.println("At " + stage + " and currently using " + memUsage + "MB of memory");
        }
    }

}
