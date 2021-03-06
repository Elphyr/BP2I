package BP2I.IntegrationServeurCollecte.Func;

import BP2I.IntegrationServeurCollecte.Utils.IntegrationParams;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.collect.Collections2;
import org.spark_project.guava.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MiscFunctions {

    public static List<Path> getFilesPath(Path path) throws IOException {

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
     *
     * @param listOfPaths
     * @return
     */
    static Path getDesFilePath(List<Path> listOfPaths) {

        List<String> listOfFiles = new ArrayList<>();

        for (Path path : listOfPaths) {

            listOfFiles.add(path.toString());
        }

        Path desPath = new Path(Collections2.filter(listOfFiles, Predicates.containsPattern(".des")).toString().replace("[", "").replace("]", ""));

        return desPath;
    }

    /**
     * Goal: read the .des file and extract all the types in a list.
     *
     * @param fileAbsolutePath
     * @return
     * @throws IOException
     */
    static List<String> getTypesFromDesFile(String fileAbsolutePath) throws IOException {

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
     *
     * @param paramPath
     * @return
     * @throws IOException
     */
    static List<String> getFileNameFromParameter(String paramPath) throws IOException {

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
     *
     * @param paramPath
     * @return
     * @throws IOException
     */
    static List<String> getTypesFromParameter(String paramPath) throws IOException {

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
     *
     * @param args
     * @return
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static String initializeReport(String[] args) throws IOException, SQLException, ClassNotFoundException {

        String tableName = Lists.reverse(Arrays.asList(Arrays.asList(args).get(0).split("/"))).get(0).replace("-", "");

        String reportName = Arrays.asList(args).get(2) + "_" + tableName + "_" + IntegrationParams.dateFormatForOutside.format(IntegrationParams.date);

        JDBCFunctions.dropTable(reportName);

        return reportName;
    }

    public static void moveToGood(List<Path> goodPaths, String goodDir) throws IOException {

        for (Path p : goodPaths) {

            String tableName = p.getParent().getName();

            FileUtils.copyDirectory(new File(p.getParent().toUri().getRawPath()), new File(goodDir + "/" + tableName));
        }
    }

    public static void moveToBad(List<Path> badPaths, List<Path> goodPaths, String badDir) throws IOException {

        for (Path p : badPaths) {

            if (!goodPaths.contains(p)) {

                String tableName = p.getParent().getName();

                FileUtils.copyDirectory(new File(p.getParent().toUri().getRawPath()), new File(badDir + "/" + tableName));
            }
        }
    }
}