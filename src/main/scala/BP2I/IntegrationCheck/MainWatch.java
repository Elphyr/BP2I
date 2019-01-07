package BP2I.IntegrationCheck;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.WatchEvent.Kind;
import java.sql.SQLException;
import java.util.Objects;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.*;

public class MainWatch {

    private static void watchDirectoryPath(Path path) throws IOException {

        IntegrationProperties prop = new IntegrationProperties();

        // Sanity check - Check if path is a folder
        try {

            Boolean isFolder = (Boolean) Files.getAttribute(path, "basic:isDirectory", NOFOLLOW_LINKS);

            if (!isFolder) {

                throw new IllegalArgumentException("Path: " + path + " is not a folder");
            }

        } catch (IOException ioe) {

            ioe.printStackTrace();
        }

        System.out.println("Watching path: " + path);

        FileSystem fs = path.getFileSystem();

        try (WatchService service = fs.newWatchService()) {

            // We register the path to the service
            // We watch for creation events
            path.register(service, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);

            // Start the infinite polling loop
            WatchKey key = null;

            do {

                key = service.take();

                // Dequeue events
                Kind<?> kind = null;

                for (WatchEvent<?> watchEvent : key.pollEvents()) {

                    // Get the type of the event
                    kind = watchEvent.kind();

                    if (OVERFLOW != kind) {

                        if (ENTRY_CREATE == kind) {

                            Path newPath = ((WatchEvent<Path>) watchEvent).context();

                            System.out.println("New path created: " + newPath);

                            org.apache.hadoop.fs.Path tableParamPath = new org.apache.hadoop.fs.Path(prop.tabParamDir);

                            org.apache.hadoop.fs.Path dirPath = new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString() + "/" + newPath.toString());

                            if (Objects.requireNonNull(new File(dirPath.toUri().getRawPath()).list()).length != 0) {

                                try {
                                    new IntegrationChecks().integrationChecksSingleFolder(dirPath, tableParamPath, dirPath.getParent().getName());

                                } catch (SQLException | ClassNotFoundException e) {
                                    e.printStackTrace();
                                }
                            } else {

                                System.out.println("WARNING: no file found inside directory: '" + dirPath.toString() + "', please check.");
                            }
                        }
                    } else if (ENTRY_MODIFY == kind) {

                        Path newPath = ((WatchEvent<Path>) watchEvent).context();

                        System.out.println("New path modified: " + newPath);
                    } else if (ENTRY_DELETE == kind) {

                        Path newPath = ((WatchEvent<Path>) watchEvent).context();

                        System.out.println("Path deleted: " + newPath);
                    }
                }

                if (!key.reset()) {

                    break;
                }
            } while (true);
        } catch (IOException | InterruptedException ioe) {

            ioe.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {

        File dir = new File(args[0]);

        Environment.setEnvironment(args);

        watchDirectoryPath(dir.toPath());
    }
}