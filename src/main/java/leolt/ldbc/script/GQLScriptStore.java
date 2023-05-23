package leolt.ldbc.script;

import java.io.IOException;
import java.nio.charset.Charset;
import javafx.util.Pair;
import org.apache.commons.io.IOUtils;

public class GQLScriptStore {

    /**
     * Get gql script from resource files.
     * @param resourcePath The resource path script stored.
     * @return return gql script from resource file.
     */
    private static String getScript(String resourcePath) throws IOException {
        return IOUtils.resourceToString(resourcePath, Charset.defaultCharset());
    }

    public static String getBi01Gql() throws IOException {
        return getScript("/ldbc/bi_01.sql");
    }

    public static String getBi02Gql() throws IOException {
        return getScript("/ldbc/bi_02.sql");
    }

    public static String getBi03Gql() throws IOException {
        return getScript("/ldbc/bi_03.sql");
    }

    public static String getBi04Gql() throws IOException {
        return getScript("/ldbc/bi_04.sql");
    }

    public static String getBi05Gql() throws IOException {
        return getScript("/ldbc/bi_05.sql");
    }


    public static String getBi06Gql() throws IOException {
        return getScript("/ldbc/bi_06.sql");
    }

    public static String getBi07Gql() throws IOException {
        return getScript("/ldbc/bi_07.sql");
    }

    public static String getBi08Gql() throws IOException {
        return getScript("/ldbc/bi_08.sql");
    }

    public static String getBi09Gql() throws IOException {
        return getScript("/ldbc/bi_09.sql");
    }

    public static String getBi10Gql() throws IOException {
        return getScript("/ldbc/bi_10.sql");
    }


    public static String getBi11Gql() throws IOException {
        return getScript("/ldbc/bi_11.sql");
    }

    public static String getBi12Gql() throws IOException {
        return getScript("/ldbc/bi_12.sql");
    }

    public static String getBi13Gql() throws IOException {
        return getScript("/ldbc/bi_13.sql");
    }

    public static String getBi14Gql() throws IOException {
        return getScript("/ldbc/bi_14.sql");
    }

    public static String getBi15Gql() throws IOException {
        return getScript("/ldbc/bi_15.sql");
    }


    public static String getBi16Gql() throws IOException {
        return getScript("/ldbc/bi_16.sql");
    }

    public static String getBi17Gql() throws IOException {
        return getScript("/ldbc/bi_17.sql");
    }

    public static String getBi18Gql() throws IOException {
        return getScript("/ldbc/bi_18.sql");
    }

    public static String getBi19Gql() throws IOException {
        return getScript("/ldbc/bi_19.sql");
    }

    public static String getBi20Gql() throws IOException {
        return getScript("/ldbc/bi_20.sql");
    }
}
