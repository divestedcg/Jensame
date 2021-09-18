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
    private static final int GC_INTERVAL_HIGH = 100000;
    private static final int MAX_THREAD_COUNT = 8;
    private static ThreadPoolExecutor threadPoolExecutorFind = null;
    private static ThreadPoolExecutor threadPoolExecutorWork = null;
    private static int duplicateFiles = 0;
    private static File fdupesOut = null;
    private static long originalMountTotalSize = 0;
    private static final AtomicInteger TOTAL_FILES = new AtomicInteger();
    private static final AtomicInteger TOTAL_DIRS = new AtomicInteger();
    private static final AtomicInteger FILES_READ = new AtomicInteger();
    private static final AtomicLong DATA_READ = new AtomicLong();
    private static final ConcurrentLinkedQueue<Future<?>> futures = new ConcurrentLinkedQueue<>();
    private static final ConcurrentHashMap<Long, ConcurrentSkipListSet<String>> FILE_SIZES = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Long, ConcurrentSkipListSet<String>> FILE_HASHES = new ConcurrentHashMap<>();
    private static final ArrayList<String> FDUPES_CONTENTS = new ArrayList<>();

    public static void main(String[] args) {
        final long startTime = System.currentTimeMillis();
        long startTimeSub = System.currentTimeMillis();

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
        threadPoolExecutorFind = (ThreadPoolExecutor) Executors.newFixedThreadPool(getMaxThreads(true));
        threadPoolExecutorFind.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolExecutorWork = (ThreadPoolExecutor) Executors.newFixedThreadPool(getMaxThreads(true));
        threadPoolExecutorWork.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        for (int c = 1; c < args.length; c++) {
            if (args[c] != null) {
                startTimeSub = System.currentTimeMillis();
                processDirectory(new File(args[c]));
                waitForThreadsComplete();
                System.out.println("Gathered and started hashing files " + args[c] + " in " + getTime(startTimeSub));
            }
        }

        startTimeSub = System.currentTimeMillis();
        processDuplicateSizes(); //Queue all files with equal size for hashing
        waitForThreadsComplete();
        System.out.println("Finished hashing all potential duplicates in " + getTime(startTimeSub));

        startTimeSub = System.currentTimeMillis();
        processDuplicateHashes(); //Hash all queued files
        System.out.println("Identified all duplicates in " + getTime(startTimeSub));

        writeFdupes(); //Write out the fdupes
        printFinalStats(startTime); //Status
        System.exit(0); //Exit
    }

    private static void waitForThreadsComplete() {
        //while (threadPoolExecutor.getActiveCount() > 0 && futures.size() > 0) {}
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        printMemUsageGc("thread pool emptied");
    }

    private static void processDirectory(File dir) {
        if (!dir.exists()) {
            System.out.println("Path doesn't exist: " + dir);
        } else {
            originalMountTotalSize = dir.getTotalSpace();
            printMemUsage("finding files");
            findFilesRecursive(dir);
            printMemUsageGc("found files");
        }
    }

    public static void findFilesRecursive(File root) {
        File[] files = root.listFiles();
        if (files != null && files.length > 0) {
            for (File f : files) {
                if (f.canRead() && !Files.isSymbolicLink(f.toPath())) {
                    if (f.isDirectory() && (f.getTotalSpace() == originalMountTotalSize)) {
                        if ((TOTAL_DIRS.getAndIncrement() % GC_INTERVAL) == 0) {
                            printMemUsageGc("recursed " + TOTAL_DIRS + " directories");
                        }
                        futures.add(threadPoolExecutorFind.submit(() -> findFilesRecursive(f)));
                    } else {
                        if (Files.isRegularFile(f.toPath()) && f.length() >= MINIMUM_FILE_SIZE && f.length() <= MAXIMUM_FILE_SIZE) {
                            if(FILE_SIZES.containsKey(f.length())) {
                                for (String file : FILE_SIZES.get(f.length())) {
                                    FILE_SIZES.get(f.length()).remove(file);
                                    futures.add(threadPoolExecutorWork.submit(() -> getFileHash(new File(file))));
                                }
                                futures.add(threadPoolExecutorWork.submit(() -> getFileHash(f)));
                            } else {
                                FILE_SIZES.putIfAbsent(f.length(), new ConcurrentSkipListSet<>());
                                FILE_SIZES.get(f.length()).add(f.toString());
                            }
                        }
                        if ((TOTAL_FILES.getAndIncrement() % GC_INTERVAL_HIGH) == 0) {
                            printMemUsageGc("found " + TOTAL_FILES + " files");
                        }
                    }
                }
            }
        }
    }

    private static void processDuplicateSizes() {
        for (Map.Entry<Long, ConcurrentSkipListSet<String>> sameFiles : FILE_SIZES.entrySet()) {
            if (sameFiles.getValue().size() > 1) {
                for (String file : sameFiles.getValue()) {
                    futures.add(threadPoolExecutorWork.submit(() -> getFileHash(new File(file))));
                }
            }
            FILE_SIZES.remove(sameFiles.getKey());
        }
        printMemUsage("processed duplicate sizes");
        FILE_SIZES.clear();
        printMemUsageGc("processed duplicate sizes");
    }


    private static void processDuplicateHashes() {
        for (Map.Entry<Long, ConcurrentSkipListSet<String>> sameFiles : FILE_HASHES.entrySet()) {
            if (sameFiles.getValue().size() > 1) {
                duplicateFiles += sameFiles.getValue().size();
                //System.out.println("Duplicates of " + sameFiles.getKey() + ": " + Arrays.toString(sameFiles.getValue().toArray()));
                FDUPES_CONTENTS.addAll(sameFiles.getValue());
                FDUPES_CONTENTS.add("");
                if ((duplicateFiles % GC_INTERVAL) == 0) {
                    printMemUsageGc("processing duplicate hashes " + duplicateFiles);
                }
            }
            FILE_HASHES.remove(sameFiles.getKey());
        }
        printMemUsage("process duplicate hashes");
        FILE_HASHES.clear();
        printMemUsageGc("process duplicate hashes");
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
            if ((FILES_READ.getAndIncrement() % GC_INTERVAL) == 0) {
                printMemUsageGc("hashed " + FILES_READ + " files");
            }
            DATA_READ.getAndAdd(file.length());
            //System.out.println(file.toString() + " - " + hash);
            FILE_HASHES.putIfAbsent(hash, new ConcurrentSkipListSet<>());
            FILE_HASHES.get(hash).add(file.toString());
        } catch (Exception e) {
            //e.printStackTrace();
        }
    }

    private static void writeFdupes() {
        if (duplicateFiles > 0) {
            writeArrayToFile(fdupesOut, FDUPES_CONTENTS);
            printMemUsage("fdupes output");
            FDUPES_CONTENTS.clear();
            printMemUsageGc("fdupes output");
        }
    }

    private static void printFinalStats(long startTime) {
        long mbRead = DATA_READ.longValue() / 1000L / 1000L;
        long msSpent = System.currentTimeMillis() - startTime;
        long mbPerSecond = mbRead;
        if (msSpent > 1000) {
            mbPerSecond = (long) (((double) mbRead) / ((double) msSpent / 1000D));
        }
        System.out.println("Found " + TOTAL_FILES + " files, hashed " + FILES_READ + " files, totalling " + mbRead + "MB, and identified " + duplicateFiles + " duplicates in " + msSpent + "ms at " + mbPerSecond + "MBps");
        printMemUsage("finish");
    }

    public static int getMaxThreads(boolean cap) {
        int maxThreads = Runtime.getRuntime().availableProcessors();
        if (cap && maxThreads > MAX_THREAD_COUNT) {
            maxThreads = MAX_THREAD_COUNT;
        }
        return maxThreads;
    }

    public static String getTime(long startTime) {
        return (System.currentTimeMillis() - startTime) + "ms";
    }

    public static void printMemUsage(String stage) {
        if (DEBUG) {
            long memUsage = ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024);
            System.out.println("At " + stage + " and currently using " + memUsage + "MB of memory");
        }
    }

    public static void printMemUsageGc(String stage) {
        printMemUsage(stage);
        System.gc();
        printMemUsage(stage + " post-gc");
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
}
