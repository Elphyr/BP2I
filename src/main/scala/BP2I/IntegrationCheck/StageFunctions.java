package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.collect.Collections2;
import org.spark_project.guava.collect.Lists;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class StageFunctions {

    private MiscFunctions mf = new MiscFunctions();
    private JDBCFunctions jdbc = new JDBCFunctions();
    private JavaParam jvp = new JavaParam();

    /**
     * Show the stage on the screen.
     * @param stageNbr
     */
    void showStage(int stageNbr) {

        String stageDes = "";
        if (stageNbr == 0) stageDes = "Check if types written in parameter table are usable in the datalake.";
        else if (stageNbr == 1) stageDes = "Check if all files have the right name.";
        else if (stageNbr == 2) stageDes = "Check if any file already exists in the datalake.";
        else if (stageNbr == 3) stageDes = "Check if all files are here (.dat & .des).";
        else if (stageNbr == 4) stageDes = "Check if all types in the .des file are accepted in the datalake.";

        System.out.println("\n" +
                "======================== ############## ========================" + "\n" +
                "=========================== STAGE " + stageNbr + " ============================" + "\n" +
                stageDes + "\n" +
                "======================== ############## ========================" + "\n");
    }

    /**
     * STAGE 0
     *
     * @param tableParamPath
     * @param reportName
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    void checkTypesInParameter(Path tableParamPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<String> listOfAcceptedTypes = jvp.acceptedTypes;

        List<String> types = mf.getTypesFromParameter(tableParamPath.toUri().getRawPath());

        List<String> listOfRefusedTypes = new ArrayList<>();

        for (String type : types) {

            if (!listOfAcceptedTypes.contains(type)) {

                listOfRefusedTypes.add(type);
            }
        }

        if (listOfRefusedTypes.isEmpty()) {

            System.out.println("Types are fine in the parameter file.");
            String line = jvp.dateFormatForInside.format(jvp.date).concat(";0;OK;");
            mf.writeInReport(reportName, line);
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "0", "OK", "", "");

        } else {

            System.out.println("Amount of types to change: " + listOfRefusedTypes.size());
            System.out.println("Types to change: " + listOfRefusedTypes);
            System.out.println("The parameter table is wrong: please correct type before going further.");

            String commentary = "Types to change: " + listOfRefusedTypes;

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";0;KO;100");
            mf.writeInReport(reportName, line);
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "0", "KO", "100", commentary);

        }
    }

    /**
     * Stage 1
     *
     * @param listOfPath
     * @param appParamPath
     * @param reportName
     * @return
     * @throws IOException
     */
    List<Path> filerFilesWrongName(List<Path> listOfPath, Path appParamPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<String> listOfExpectedFileNames = mf.getFileNameFromParameter(appParamPath.toUri().getRawPath());

        List<Path> listOfPathStage1 = new ArrayList<>();

        List<String> listOfTables = new ArrayList<>();

        List<String> listOfWrongNamedFiles = new ArrayList<>();

        for (Path path : listOfPath) {

            List<String> separatedFilesPath = Lists.reverse(
                    Arrays.asList(
                            path.toUri().getRawPath()
                                    .replace("[", "").replace("]", "")
                                    .split("/")));

            List<String> separatedFileName = Arrays.asList(separatedFilesPath.get(0).split("_"));

            try {

                listOfTables.add(separatedFileName.get(1));

                for (String s : listOfTables) {

                    if (listOfExpectedFileNames.contains(s)) {

                        listOfPathStage1.add(path);
                    } else {

                        listOfWrongNamedFiles.add(s);
                    }
                }
            } catch (ArrayIndexOutOfBoundsException e) {

                System.out.println("There is a problem here: " + path.toUri().getRawPath());
            }
        }

        if (listOfWrongNamedFiles.isEmpty()) {

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";1;OK;");
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "1", "OK", "", "");
            mf.writeInReport(reportName, line);

        } else {

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";1;KO;xxx");
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "1", "KO", "xxx", "");
            mf.writeInReport(reportName, line);

            System.out.println("MISTAKE HERE: FILE NAME NOT FOUND IN PARAMETER TABLE!");
            System.out.println(listOfWrongNamedFiles);
        }

        return listOfPathStage1.stream().distinct().collect(Collectors.toList());
    }

    /**
     * STAGE 2
     *
     * @param parentPath
     * @param hdfsPath
     * @return
     * @throws IOException
     */
    List<Path> filterFilesAlreadyExistingHdfs(Path parentPath, Path hdfsPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<Path> listOfFilesToIntegrate = mf.getFilesPath(parentPath);

        List<Path> listOfPathStage2 = new ArrayList<>();

        List<String> listOfFileNamesHDFS = mf.getFilesPath(hdfsPath).stream().map(Path::getName).collect(Collectors.toList());

        for (Path p : listOfFilesToIntegrate) {

            if (listOfFileNamesHDFS.contains(p.getName())) {

                System.out.println("WARN: " + p.getName() + " ALREADY EXISTS IN THE DATALAKE!");
            } else {

                listOfPathStage2.add(p);
            }
        }

        if (listOfFileNamesHDFS.isEmpty()) {

            System.out.println("All files already exist in the datalake.");

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";1;KO;101");

            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "2", "KO", "101", "");
            mf.writeInReport(reportName, line);

        } else {

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";1;OK;");
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "2", "OK", "", "");

            mf.writeInReport(reportName, line);
        }

        return listOfPathStage2;
    }

    /**
     * STAGE 3
     *
     * @param listOfPath
     * @param reportName
     * @return
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    List<Path> filterFilesWithoutDatDes(List<Path> listOfPath, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<Path> listOfPathStage3 = new ArrayList<>();

        List<String> flag = new ArrayList<>();
        List<String> commentary = new ArrayList<>();

        for (Path path : listOfPath) {

            List<String> files = mf.getFilesPath(path.getParent()).stream().map(Path::toString).collect(Collectors.toList());

            Boolean condDat = !Collections2.filter(files, Predicates.containsPattern(".dat")).isEmpty();
            Boolean condDes = !Collections2.filter(files, Predicates.containsPattern(".des")).isEmpty();

            if (condDat && condDes) {

                System.out.println(path.getName() + " is good!");
                listOfPathStage3.add(path);

            } else if (condDat) {

                System.out.println(path.getName() + " lack its buddy .des file!");
                commentary.add(path.getName() + " lack its buddy .des file!");
                flag.add("110");

            } else if (condDes) {

                System.out.println(path.getName() + " lack its buddy .dat file!");
                commentary.add(path.getName() + " lack its buddy .dat file!");
                flag.add("111");

            } else {

                System.out.println("No description and data file found!");
                flag.add("112");
            }
        }

        if (flag.isEmpty()) {

            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "3", "OK", "", "");
            String line = jvp.dateFormatForInside.format(jvp.date).concat(";3;OK;");
            mf.writeInReport(reportName, line);
        } else {

            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "3", "KO", flag.get(0), commentary.toString());
            String line = jvp.dateFormatForInside.format(jvp.date).concat(";3;KO;" + flag.get(0));
            mf.writeInReport(reportName, line);
        }

        return listOfPathStage3;
    }

    /**
     * STAGE 4
     *
     * @param listOfPaths
     * @param reportName
     * @return
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    List<Path> filterFilesWithoutAllowedTypes(List<Path> listOfPaths, String reportName) throws IOException, SQLException, ClassNotFoundException {

        List<String> listOfAcceptedTypes = jvp.acceptedTypes;

        List<String> listOfFilesToRemove = new ArrayList<>();

        List<Path> listOfPathStage4 = new ArrayList<>();

        for (Path path : listOfPaths) {

            Path desPath = mf.getDesFilePath(mf.getFilesPath(path.getParent()));

            List<String> listOfTypes = mf.getTypesFromDesFile(desPath.toUri().getRawPath());

            for (String type : listOfTypes) {

                if (!listOfAcceptedTypes.contains(type)) {

                    listOfFilesToRemove.add(path.getName());
                }
            }

            if (!listOfFilesToRemove.contains(path.getName())) {

                listOfPathStage4.add(path);
            }
        }

        if (listOfFilesToRemove.isEmpty()) {

            System.out.println("Types are fine in all tables.");
            String line = jvp.dateFormatForInside.format(jvp.date).concat(";4;OK;");
            mf.writeInReport(reportName, line);
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "4", "OK", "", "");

        } else {

            System.out.println("Amount of types to change: " + listOfFilesToRemove.size());
            System.out.println("Types to change: " + listOfFilesToRemove);

            String line = jvp.dateFormatForInside.format(jvp.date).concat(";4;KO;100");
            mf.writeInReport(reportName, line);
            jdbc.writeStageResultIntoTable(reportName, jvp.dateFormatForInside.format(jvp.date), "4", "KO", "100", "");
        }

        return listOfPathStage4;
    }
}
