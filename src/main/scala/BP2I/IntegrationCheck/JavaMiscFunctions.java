package BP2I.IntegrationCheck;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.collect.Collections2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class JavaMiscFunctions {

    List<Path> getFilesPath(Path path) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        RemoteIterator<LocatedFileStatus> directories = fs.listFiles(path, true);

        List<Path> listOfPath = new ArrayList<Path>();

        while (directories.hasNext()) {
            LocatedFileStatus fileStatus = directories.next();

            listOfPath.add(fileStatus.getPath());
        }

        return listOfPath;
    }

    Path getDesFilePath(List<Path> listOfPaths) {

        List<String> listOfFiles = new ArrayList<String>();

        for (Path p : listOfPaths) {

            listOfFiles.add(p.toString());
        }

        String toto = Collections2.filter(listOfFiles, Predicates.containsPattern(".des")).toString().replace("[", "").replace("]", "");

        Path desPath = new Path(toto);

        return desPath;

    }

    Boolean checkDatExists(List<Path> listOfPaths) {

        List<String> listOfFiles = new ArrayList<String>();

        for (Path p : listOfPaths) {

            listOfFiles.add(p.toString());
        }

        boolean cond = !Collections2.filter(listOfFiles, Predicates.containsPattern(".dat")).isEmpty();

        return cond;
    }

    Boolean checkDesExists(List<Path> listOfPaths) {

        List<String> listOfFiles = new ArrayList<String>();

        for (Path p : listOfPaths) {

            listOfFiles.add(p.toString());
        }

        boolean cond = !Collections2.filter(listOfFiles, Predicates.containsPattern(".des")).isEmpty();

        return cond;
    }

    List<String> getColumnsFromDesFile(String fileAbsolutePath) throws IOException {

        File file = new File(fileAbsolutePath);

        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

        List<String> listOfColumns = new ArrayList<String>();

        for (String line : lines) {

            String[] array = line.split(";", -1);

            listOfColumns.add(array[0]);
        }

        listOfColumns.remove(0);

        return listOfColumns;
    }
}