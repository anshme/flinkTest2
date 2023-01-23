package org.example;

import org.apache.tools.tar.*;
import java.io.*;

import java.nio.file.*;
import java.util.zip.GZIPInputStream;
public class UnTargz {
    private static final String RESERVED_CHAR = "[<>:/\"///|?/*]";

    public static void main(String... args) throws IOException {
        // the example file "foo.tar.gz" contains the files
        // "foo¥_\.file"
        // "foo¥_?.file"

        extract(Paths.get("C:/Users/mpurn/Downloads/yeast_orfs.new.20040422.gz"));
    }

    public static void extract(Path path) throws IOException {

        if (!path.toString().endsWith(".gz") && !path.toString().endsWith(".tgz")) {
            throw new Error("extension must be tar.gz.");
        }

        try (TarInputStream tin = new TarInputStream(new GZIPInputStream(Files.newInputStream(path.toFile().toPath())))) {
            for (TarEntry tarEnt = tin.getNextEntry(); tarEnt != null; tarEnt = tin.getNextEntry()) {
                String entryName = tarEnt.getName();
                System.out.println("tarEnt = " + entryName);
                /*

                replace all reserved characters
                http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247%28v=vs.85%29.aspx

                < (less than)
                > (greater than)
                : (colon)
                " (double quote)
                / (forward slash)
                \ (backslash)
                | (vertical bar or pipe)
                ? (question mark)
                * (asterisk)

                */

                //String sanitized = entryName.replaceAll(RESERVED_CHAR, "_");
                String sanitized = entryName.replaceAll(RESERVED_CHAR, "_");
                // check is needed to check if directory/file already exist
                if (tarEnt.isDirectory()) {
                    new File(sanitized).mkdir();
                } else {
                    try (FileOutputStream fos = new FileOutputStream(new File(sanitized))) {
                        tin.copyEntryContents(fos);
                    }
                }
            }
        }
    }
}
