import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.Security;
import java.util.Arrays;
import java.util.HashMap;

public class Main {

    private final HashMap<File, String> fileHashesMD5 = new HashMap<>();

    public static void main(String[] args) {
        System.out.println(Arrays.toString(Security.getProviders()));
    }

    private void getFileHash(File file) {
        try {
            InputStream fis = new FileInputStream(file);

            byte[] buffer = new byte[4096];
            int numRead;

            MessageDigest digestMD5 = MessageDigest.getInstance("MD5");

            do {
                numRead = fis.read(buffer);
                if (numRead > 0) {
                    digestMD5.update(buffer, 0, numRead);
                }
            } while (numRead != -1);

            fis.close();

            fileHashesMD5.put(file, String.format("%032x", new BigInteger(1, digestMD5.digest())));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
