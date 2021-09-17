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

import java.io.*;
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

    private static ThreadPoolExecutor threadPoolExecutor = null;
    private static final ConcurrentHashMap<File, Long> fileHashes = new ConcurrentHashMap<>();
    private static AtomicInteger filesRead = new AtomicInteger();
    private static AtomicLong dataRead = new AtomicLong();
    private static int duplicateFiles = 0;
    private static File fdupesOut = null;
    private static ArrayList<String> fdupesContents = new ArrayList<>();

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
                    hashFilesRecursive(recurse);
                }
            }
        }

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
                //System.out.println("Duplicates of " + sameFiles.getKey() + ": " + Arrays.toString(sameFiles.getValue().toArray()));
                for (File file : sameFiles.getValue()) {
                    fdupesContents.add(file.toString());
                }
                fdupesContents.add("");
            }
        }

        //Write out the fdupes
        if(duplicateFiles > 0) {
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
                        //131,072 (128*1024) is the default minimum block size of duperemove
                        //4096 (4*1024) is the absolute minimum
                        //1,048,576 (1024*1024) is the absolute maximum
                        //https://github.com/markfasheh/duperemove/blob/548fc5ea76f97024c4ba90cff7bd8ff7bd36f9e5/duperemove.c#L50
                        if (Files.isRegularFile(f.toPath()) && f.canRead() && f.length() >= 131072) {
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
        if (maxTheads > 8) {
            maxTheads = 8;
        }
        return maxTheads;
    }

    //Credit (CC BY-SA 4.0): https://stackoverflow.com/a/64169740
    public static Path mountOf(Path p) throws IOException {
        FileStore fs = Files.getFileStore(p);
        Path temp = p.toAbsolutePath();
        Path mountp = temp;

        while ((temp = temp.getParent()) != null && fs.equals(Files.getFileStore(temp))) {
            mountp = temp;
        }
        return mountp;
    }

}
