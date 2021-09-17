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
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    private static final boolean DEBUG = false;
    private static ThreadPoolExecutor threadPoolExecutor = null;
    private static final ConcurrentHashMap<String, Long> fileHashes = new ConcurrentHashMap<>();
    private static final AtomicInteger filesRead = new AtomicInteger();
    private static final AtomicLong dataRead = new AtomicLong();
    private static int duplicateFiles = 0;
    private static File fdupesOut = null;
    private static final ArrayList<String> fdupesContents = new ArrayList<>();
    private static long originalMountTotalSize = 0;
    private static final long minimumFileSize = (32L * 1024L); //32KB
    private static final long maximumFileSize = (1024L * 1024L * 1024L * 10L); //10GB

    public static void main(String[] args) {
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
        printMemUsage("init");

        //Start the hashing
        long startTime = System.currentTimeMillis();
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newScheduledThreadPool(getMaxThreads());
        for (int c = 1; c < args.length; c++) {
            File recurse = null;
            if (args[c] != null) {
                recurse = new File(args[c]);
                if (!recurse.exists()) {
                    System.out.println("Path doesn't exist: " + recurse);
                } else {
                    originalMountTotalSize = recurse.getTotalSpace();
                    printMemUsage("pre-hash");
                    hashFilesRecursive(recurse);
                    printMemUsage("post-hash");
                    System.gc();
                    printMemUsage("post-hash post-gc");
                }
            }
        }

        //Wait for hashing to complete //TODO: improve this
        while (threadPoolExecutor.getActiveCount() > 0) {
        }
        if (getMaxThreads() == 1) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        printMemUsage("hash finished");

        //Identify all duplicate files
        HashMap<Long, List<String>> hashedFiles = new HashMap<>();
        for (Map.Entry<String, Long> sets : fileHashes.entrySet()) {
            List<String> emptyList = new ArrayList<>();
            if (hashedFiles.containsKey(sets.getValue())) {
                emptyList.addAll(hashedFiles.get(sets.getValue()));
            }
            emptyList.add(sets.getKey());
            hashedFiles.put(sets.getValue(), emptyList);
        }
        printMemUsage("duplicate identification");
        fileHashes.clear();
        System.gc();
        printMemUsage("duplicate identification post-gc");

        //Output the duplicates
        for (Map.Entry<Long, List<String>> sameFiles : hashedFiles.entrySet()) {
            if (sameFiles.getValue().size() > 1) {
                duplicateFiles += sameFiles.getValue().size();
                //System.out.println("Duplicates of " + sameFiles.getKey() + ": " + Arrays.toString(sameFiles.getValue().toArray()));
                for (String string : sameFiles.getValue()) {
                    fdupesContents.add(string);
                }
                fdupesContents.add("");
            }
        }
        printMemUsage("duplicate output");
        hashedFiles.clear();
        System.gc();
        printMemUsage("duplicate output post-gc");

        //Write out the fdupes
        if (duplicateFiles > 0) {
            writeArrayToFile(fdupesOut, fdupesContents);
            fdupesContents.clear();
            System.gc();
            printMemUsage("fdupes output");
        }

        //Status
        long mbRead = dataRead.longValue() / 1000L / 1000L;
        long msSpent = System.currentTimeMillis() - startTime;
        long mbPerSecond = mbRead;
        if (msSpent > 1000) {
            mbPerSecond = mbRead / (msSpent / 1000);
        }
        System.out.println("Hashed " + filesRead + " files, totalling " + mbRead + "MB, and identified " + duplicateFiles + " duplicates in " + msSpent + "ms at " + mbPerSecond + "MBps");
        printMemUsage("finish");

        //Exit
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
                if (f.canRead() && !Files.isSymbolicLink(f.toPath())) {
                    if (f.isDirectory() && (f.getTotalSpace() == originalMountTotalSize)) {
                        hashFilesRecursive(f);
                    } else {
                        if (Files.isRegularFile(f.toPath()) && f.length() >= minimumFileSize && f.length() <= maximumFileSize) {
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
            if(filesRead.getAndIncrement() % 10000 == 0) {
                printMemUsage("hashing 10k");
                System.gc();
                printMemUsage("hashing 10k post-gc");
            }
            dataRead.getAndAdd(file.length());
            //System.out.println(file.toString() + " - " + hash);
            fileHashes.put(file.toString(), hash);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getMaxThreads() {
        int maxThreads = Runtime.getRuntime().availableProcessors();
        if (maxThreads > 8) {
            maxThreads = 8;
        }
        return maxThreads;
    }

    public static void printMemUsage(String stage) {
        if(DEBUG) {
            long memUsage = ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024);
            System.out.println("At " + stage + " and currently using " + memUsage + "MB of memory");
        }
    }

}
