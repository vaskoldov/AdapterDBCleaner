package ru.hemulen.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Класс содержит методы для манипулирования данными в схеме CORE
 */
public class CORE {
    private Statement statement;
    private String threshold;

    public CORE(Connection connection, String threshold) throws SQLException {
        statement = connection.createStatement();
        this.threshold = threshold;
    }

    public void createTempTables() throws SQLException {
        dropTempTables();
        String sql = "CREATE TABLE CORE.REQ_TMP (ID UUID NOT NULL, constraint REQ_TMP_ID_PK primary key (ID));\n" +
                "CREATE TABLE CORE.RESP_TMP (ID UUID NOT NULL, constraint RESP_TMP_ID_PK primary key (ID));";
        statement.executeUpdate(sql);
    }

    public void dropTempTables() throws SQLException {
        String sql = "DROP TABLE IF EXISTS CORE.REQ_TMP;\n" +
                "DROP TABLE IF EXISTS CORE.RESP_TMP;";
        statement.executeUpdate(sql);
    }

    public void fillTempTables() throws SQLException {
        String sql = "INSERT INTO CORE.REQ_TMP\n" +
                "SELECT DISTINCT MM.REFERENCE_ID\n" +
                "FROM CORE.MESSAGE_METADATA MM\n" +
                "  LEFT JOIN CORE.MESSAGE_CONTENT MC ON MM.ID = MC.ID\n" +
                "WHERE MM.MESSAGE_TYPE = 'RESPONSE'\n" +
                "  AND MM.REFERENCE_ID IS NOT NULL\n" +
                "  AND MM.CREATION_DATE <= '" + threshold + "'\n" +
                "  AND MC.MODE <> 'STATUS';";
        statement.executeUpdate(sql);
        sql = "INSERT INTO CORE.RESP_TMP\n" +
                "SELECT ID\n" +
                "FROM CORE.MESSAGE_METADATA\n" +
                "WHERE REFERENCE_ID IN (\n" +
                "    SELECT ID FROM CORE.REQ_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "INSERT INTO CORE.RESP_TMP\n" +
                "SELECT ID\n" +
                "FROM CORE.MESSAGE_METADATA\n" +
                "WHERE MESSAGE_TYPE = 'RESPONSE'\n" +
                "   AND REFERENCE_ID IS NULL;";
        statement.executeUpdate(sql);
    }

    public void deleteAttachmentMetadata() throws SQLException {
        String sql = "DELETE \n" +
                "FROM CORE.ATTACHMENT_METADATA\n" +
                "WHERE MESSAGE_METADATA_ID IN (\n" +
                "    SELECT ID FROM CORE.RESP_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE \n" +
                "FROM CORE.ATTACHMENT_METADATA\n" +
                "WHERE MESSAGE_METADATA_ID IN (\n" +
                "    SELECT ID FROM CORE.REQ_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
    }

    public void deleteMessageContent() throws SQLException {
        String sql = "DELETE\n" +
                "FROM CORE.MESSAGE_CONTENT\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM CORE.RESP_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE\n" +
                "FROM CORE.MESSAGE_CONTENT\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM CORE.REQ_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
    }

    public void deleteMessageMetadata() throws SQLException {
        String sql = "DELETE\n" +
                "FROM CORE.MESSAGE_METADATA\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM CORE.RESP_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE\n" +
                "FROM CORE.MESSAGE_METADATA\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM CORE.REQ_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
    }

    public void deleteMessageState() throws SQLException {
        String sql = "DELETE\n" +
                "FROM CORE.MESSAGE_STATE\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM CORE.RESP_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE\n" +
                "FROM CORE.MESSAGE_STATE\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM CORE.REQ_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
    }

    public void close() {
        // База данных закрывается с очисткой всех пустых областей файла базы данных
        try {
            String sql = "SHUTDOWN DEFRAG;";
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            // Здесь появится сообщение "База данных уже закрыта (чтобы отключить автоматическое закрытие
            // базы данных при останове JVM, добавьте ";DB_CLOSE_ON_EXIT=FALSE" в URL)"
            // Ничего не делаем
        }
    }

}
