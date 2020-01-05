package ru.hemulen.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Класс содержит методы для манипулирования данными в схеме UI
 */
public class UI {
    private Statement statement;
    private String threshold;

    public UI(Connection connection, String threshold) throws SQLException {
        statement = connection.createStatement();
        this.threshold = threshold;
    }

    public void createTempTables() throws SQLException {
        dropTempTables();
        String sql = "CREATE TABLE UI.BI_TMP (ID UUID NOT NULL, constraint BI_TMP_ID_PK primary key (ID));\n" +
                "CREATE TABLE UI.REQ_TMP (ID UUID NOT NULL, constraint REQ_TMP_ID_PK primary key (ID));\n" +
                "CREATE TABLE UI.RESP_TMP (ID UUID NOT NULL, constraint RESP_TMP_ID_PK primary key (ID));";
        statement.executeUpdate(sql);
    }

    public void fillTempTables() throws SQLException {
        // Извлекаем идентификаторы транзакций, в которых до отсечки получены ответы, отличные от STATUS
        String sql = "INSERT INTO UI.BI_TMP\n" +
                "SELECT DISTINCT BI.ID\n" +
                "FROM UI.BUSINESS_INTERACTION BI\n" +
                "LEFT JOIN UI.BUSINESS_MESSAGE BM ON BI.ID = BM.INTERACTION_ID\n" +
                "WHERE BI.CREATION_DATE <= '" + threshold + "'\n" +
                "AND BM.TYPE = 'RESPONSE'\n" +
                "AND BM.MODE <> 'STATUS'\n" +
                "AND BM.STATE <> 'DRAFT';";
        statement.executeUpdate(sql);
        sql = "INSERT INTO UI.REQ_TMP\n" +
                "SELECT ID\n" +
                "FROM UI.BUSINESS_MESSAGE\n" +
                "WHERE TYPE = 'REQUEST'\n" +
                "AND INTERACTION_ID IN (\n" +
                "    SELECT ID FROM UI.BI_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "INSERT INTO UI.RESP_TMP\n" +
                "SELECT ID\n" +
                "FROM UI.BUSINESS_MESSAGE\n" +
                "WHERE TYPE = 'RESPONSE'\n" +
                "AND INTERACTION_ID IN (\n" +
                "    SELECT ID FROM UI.BI_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
    }

    public void deleteBusinessAttachment() throws SQLException {
        String sql = "DELETE FROM UI.BUSINESS_ATTACHMENT\n" +
                "WHERE MESSAGE_ID IN (\n" +
                "    SELECT ID FROM UI.RESP_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE FROM UI.BUSINESS_ATTACHMENT\n" +
                "WHERE MESSAGE_ID IN (\n" +
                "    SELECT ID FROM UI.REQ_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
    }

    public void deleteBusinessMessage() throws SQLException {
        String sql = "DELETE FROM UI.BUSINESS_MESSAGE\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM UI.RESP_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE FROM UI.BUSINESS_MESSAGE\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM UI.REQ_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
    }

    public void deleteBusinessInteraction() throws SQLException {
        String sql = "DELETE FROM UI.BUSINESS_INTERACTION\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM UI.BI_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
    }

    public void deleteNotification() throws SQLException {
        String sql = "TRUNCATE TABLE UI.NOTIFICATION;";
        statement.executeUpdate(sql);
    }

    public void dropTempTables() throws SQLException {
        String sql = "DROP TABLE IF EXISTS  UI.REQ_TMP;\n" +
                "DROP TABLE IF EXISTS  UI.RESP_TMP;\n" +
                "DROP TABLE IF EXISTS  UI.BI_TMP;";
        statement.executeUpdate(sql);
    }

}
