package BP2I.IntegrationCheck;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.collect.Collections2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class MiscFunctions {

    List<Path> getFilesPath(Path path) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        RemoteIterator<LocatedFileStatus> directories = fs.listFiles(path, true);

        List<Path> listOfPath = new ArrayList<>();

        while (directories.hasNext()) {
            LocatedFileStatus fileStatus = directories.next();

            listOfPath.add(fileStatus.getPath());
        }

        return listOfPath;
    }

    /**
     * Goal: from a list of paths, return the .des file path.
     * @param listOfPaths
     * @return
     */
    Path getDesFilePath(List<Path> listOfPaths) {

        List<String> listOfFiles = new ArrayList<>();

        for (Path path : listOfPaths) {

            listOfFiles.add(path.toString());
        }

        Path desPath = new Path(Collections2.filter(listOfFiles, Predicates.containsPattern(".des")).toString().replace("[", "").replace("]", ""));

        return desPath;
    }

    /**
     * Goal: read the .des file and extract all the types in a list.
     * @param fileAbsolutePath
     * @return
     * @throws IOException
     */
    List<String> getTypesFromDesFile(String fileAbsolutePath) throws IOException {

        File file = new File(fileAbsolutePath);

        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

        List<String> listOfColumns = new ArrayList<>();

        for (String line : lines) {

            String[] array = line.split(";", -1);

            listOfColumns.add(array[2]);
        }

        listOfColumns.remove(0);

        return listOfColumns.stream().distinct().collect(Collectors.toList());
    }

    /**
     * Goal: read the APPLICATION parameter file and extract all table (file) names in a list.
     * @param paramPath
     * @return
     * @throws IOException
     */
    List<String> getFileNameFromParameter(String paramPath) throws IOException {

        File file = new File(paramPath);

        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

        List<String> listOfColumns = new ArrayList<>();

        for (String line : lines) {

            String[] array = line.split(";", -1);

            listOfColumns.add(array[0]);
        }

        listOfColumns.remove(0);

        return listOfColumns.stream().distinct().collect(Collectors.toList());
    }

    /**
     * Goal: read the TABLE parameter file and extract all types in a list.
     * @param paramPath
     * @return
     * @throws IOException
     */
    List<String> getTypesFromParameter(String paramPath) throws IOException {

        File file = new File(paramPath);

        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

        List<String> listOfTypes = new ArrayList<>();

        for (String line : lines) {

            String[] array = line.split(";", -1);

            listOfTypes.add(array[2]);
        }

        listOfTypes.remove(0);

        return listOfTypes.stream().distinct().collect(Collectors.toList());
    }

    /**
     * Goal: initialize the report by creating a Postgres table.
     * @param args
     * @return
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    String initializeReport(String[] args) throws IOException, SQLException, ClassNotFoundException {

        String reportName = Arrays.asList(args).get(2) + "_" + new JavaParam().dateFormatForOutside.format(new JavaParam().date);

        java.nio.file.Path file = Paths.get(reportName);

        Files.deleteIfExists(file);

        List<String> logColumns = Collections.singletonList("date;stage;result;error_code");
        Files.write(file, logColumns, Charset.forName("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        new JDBCFunctions().dropTable(reportName);

        return reportName;
    }

    void writeInReport(String reportName, String line) throws IOException {

        java.nio.file.Path file = Paths.get(reportName);

        List<String> lines = Collections.singletonList(line);

        Files.write(file, lines, Charset.forName("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    void moveToGood(List<Path> goodPaths, String goodDir) throws IOException {

        for (Path p : goodPaths) {

            String tableName = p.getParent().getName();

            FileUtils.copyDirectory(new File(p.getParent().toUri().getRawPath()), new File(goodDir + "/" + tableName));
        }
    }

    void moveToBad(List<Path> badPaths, List<Path> goodPaths, String badDir) throws IOException {

        for (Path p : badPaths) {

            if (!goodPaths.contains(p)) {

                String tableName = p.getParent().getName();

                FileUtils.copyDirectory(new File(p.getParent().toUri().getRawPath()), new File(badDir + "/" + tableName));
            }
        }
    }
}