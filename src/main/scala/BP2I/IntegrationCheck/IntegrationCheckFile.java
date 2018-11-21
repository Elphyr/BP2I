package BP2I.IntegrationCheck;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class IntegrationCheckFile {

    public static void main(String[] args) throws IOException {

        JavaMiscFunctions mf = new JavaMiscFunctions();

        Path parentDir = new Path("./JavaTest");

        Path goodDir = new Path("./JavaTest/bool-tab");

        Path badDir = new Path("./JavaTest/ca-resource-class-error");

        System.out.println("======================== GOOD DIRECTORY ========================");

        List<Path> listOfPathGood = mf.getFilesPath(goodDir);

        System.out.println(listOfPathGood);

        System.out.println(mf.checkDesExists(listOfPathGood));
        System.out.println(mf.checkDatExists(listOfPathGood));

        Path goodDesPath = mf.getDesFilePath(listOfPathGood);

        List<String> abcd = mf.getColumnsFromDesFile(goodDesPath.toUri().getRawPath());

        System.out.println(abcd);


        System.out.println("======================== BAD DIRECTORY ========================");

        List<Path> listOfPathBad = mf.getFilesPath(badDir);

        System.out.println(listOfPathBad);

        System.out.println(mf.checkDesExists(listOfPathBad));
        System.out.println(mf.checkDatExists(listOfPathBad));

        System.out.println("========================");
    }
}