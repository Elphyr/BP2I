package BP2I.IntegrationCheck;

import com.google.common.collect.Lists;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class JavaMiscFunctions {

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

    List<String> getFilesTableName(List<Path> listOfPath) {

        List<String> listOfTables = new ArrayList<String>();

        for (Path path : listOfPath) {

            List<String> separatedFilesPath = Lists.reverse(
                    Arrays.asList(
                            path.toUri().getRawPath()
                                    .replace("[", "").replace("]", "")
                                    .split("/")));

            List<String> separatedFileName = Arrays.asList(separatedFilesPath.get(0).split("_"));

            try {

                listOfTables.add(separatedFileName.get(1));

            }
            catch (ArrayIndexOutOfBoundsException e) {

                System.out.println("There is a problem here: " + path.toUri().getRawPath());
            }
        }

        return listOfTables.stream().distinct().collect(Collectors.toList());
    }

    int getAmountOfFiles(Path dirPath) throws IOException {

        List<Path> listOfFilesPath = getFilesPath(dirPath);

        List<String> listOfFilesNames = new ArrayList<String>();

        for (Path path : listOfFilesPath) {

            List<String> separatedFilesPath = Lists.reverse(
                    Arrays.asList(
                            path.toUri().getRawPath()
                                    .replace("[", "").replace("]", "")
                                    .split("/")));

            listOfFilesNames.add(separatedFilesPath.get(0));
        }

        List<String> filteredListOfFilesNames = listOfFilesNames.stream()
                .filter(x -> x.contains(".dat") || x.contains(".des")).collect(Collectors.toList());

        return filteredListOfFilesNames.size();
    }

    Path getDesFilePath(List<Path> listOfPaths) {

        List<String> listOfFiles = new ArrayList<String>();

        for (Path path : listOfPaths) {

            listOfFiles.add(path.toString());
        }

        Path desPath = new Path(Collections2.filter(listOfFiles, Predicates.containsPattern(".des")).toString().replace("[", "").replace("]", ""));

        return desPath;
    }

    Boolean checkDatExists(List<Path> listOfPaths) {

        List<String> listOfFiles = new ArrayList<String>();

        for (Path path : listOfPaths) {

            listOfFiles.add(path.toString());
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

    List<String> getListOfTablesFromParameter(String fileAbsolutePath) throws IOException {

        File file = new File(fileAbsolutePath);

        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

        List<String> listOfColumns = new ArrayList<String>();

        for (String line : lines) {

            String[] array = line.split(";", -1);

            listOfColumns.add(array[1]);
        }

        listOfColumns.remove(0);
        listOfColumns.remove(0);

        return listOfColumns;
    }
}