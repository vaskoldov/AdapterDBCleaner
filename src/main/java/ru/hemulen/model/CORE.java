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
                "CREATE TABLE CORE.RESP1_TMP (ID UUID NOT NULL, constraint RESP1_TMP_ID_PK primary key (ID));\n" +
                "CREATE TABLE CORE.RESP2_TMP (ID UUID NOT NULL, constraint RESP2_TMP_ID_PK primary key (ID));";
        statement.executeUpdate(sql);
    }

    public void dropTempTables() throws SQLException {
        String sql = "DROP TABLE IF EXISTS CORE.REQ_TMP;\n" +
                "DROP TABLE IF EXISTS CORE.RESP1_TMP;\n" +
                "DROP TABLE IF EXISTS CORE.RESP2_TMP;";
        statement.executeUpdate(sql);
    }

    public void fillTempTables() throws SQLException {
        // В версии адаптера 3.1.1 (возможно, что в других тоже) существует два уровня ответов.
        // Наполняем REQ_TMP идентификаторами запросов, на которые до threshold пришли финальные ответы второго уровня
        String sql = "INSERT INTO CORE.REQ_TMP\n" +
                "SELECT DISTINCT REQ.ID\n" +
                "FROM CORE.MESSAGE_METADATA REQ\n" +
                "         LEFT JOIN CORE.MESSAGE_METADATA L1 ON L1.REFERENCE_ID = REQ.ID\n" +
                "         LEFT JOIN CORE.MESSAGE_METADATA L2 ON L2.REFERENCE_ID = L1.ID\n" +
                "         LEFT JOIN CORE.MESSAGE_CONTENT MC ON L2.ID = MC.ID\n" +
                "WHERE L2.MESSAGE_TYPE = 'RESPONSE'\n" +
                "  AND L1.MESSAGE_TYPE = 'RESPONSE'\n" +
                "  AND REQ.MESSAGE_TYPE = 'REQUEST'\n" +
                "  AND REQ.CREATION_DATE < '" + threshold + "'\n" +
                "  AND MC.MODE IN ('MESSAGE', 'REJECT', 'ERROR');";
        statement.executeUpdate(sql);
        // Наполняем REQ_TMP идентификаторами запросов, на которые до threshold пришли финальные ответы первого уровня
        sql =   "INSERT INTO CORE.REQ_TMP\n" +
                "SELECT DISTINCT REQ.ID\n" +
                "FROM CORE.MESSAGE_METADATA REQ\n" +
                "         LEFT JOIN CORE.MESSAGE_METADATA L1 ON L1.REFERENCE_ID = REQ.ID\n" +
                "         LEFT JOIN CORE.MESSAGE_CONTENT MC ON L1.ID = MC.ID\n" +
                "WHERE L1.MESSAGE_TYPE = 'RESPONSE'\n" +
                "  AND REQ.MESSAGE_TYPE = 'REQUEST'\n" +
                "  AND REQ.CREATION_DATE < '" + threshold + "'\n" +
                "  AND MC.MODE IN ('MESSAGE', 'REJECT', 'ERROR')\n" +
                "  AND REQ.ID NOT IN (SELECT ID FROM CORE.REQ_TMP);";
        statement.executeUpdate(sql);
        // Наполняем RESP1_TMP идентификаторами ответов первого уровня
        sql = "INSERT INTO CORE.RESP1_TMP\n" +
                "SELECT ID\n" +
                "FROM CORE.MESSAGE_METADATA\n" +
                "WHERE REFERENCE_ID IN (SELECT ID FROM CORE.REQ_TMP);";
        statement.executeUpdate(sql);
        // Наполняем RESP2_TMP идентификаторами ответов второго уровня
        sql = "INSERT INTO CORE.RESP2_TMP\n" +
                "SELECT ID\n" +
                "FROM CORE.MESSAGE_METADATA\n" +
                "WHERE REFERENCE_ID IN (SELECT ID FROM CORE.RESP1_TMP);";
        statement.executeUpdate(sql);
        // Дополняем RESP1_TMP идентификаторами ответов без ссылок на запросы
        sql = "INSERT INTO CORE.RESP1_TMP\n" +
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
                "    SELECT ID FROM CORE.RESP2_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE \n" +
                "FROM CORE.ATTACHMENT_METADATA\n" +
                "WHERE MESSAGE_METADATA_ID IN (\n" +
                "    SELECT ID FROM CORE.RESP1_TMP\n" +
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
                "    SELECT ID FROM CORE.RESP2_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE \n" +
                "FROM CORE.MESSAGE_CONTENT\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM CORE.RESP1_TMP\n" +
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
                "    SELECT ID FROM CORE.RESP2_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE \n" +
                "FROM CORE.MESSAGE_METADATA\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM CORE.RESP1_TMP\n" +
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
                "    SELECT ID FROM CORE.RESP2_TMP\n" +
                "    );";
        statement.executeUpdate(sql);
        sql = "DELETE \n" +
                "FROM CORE.MESSAGE_STATE\n" +
                "WHERE ID IN (\n" +
                "    SELECT ID FROM CORE.RESP1_TMP\n" +
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
        try {
            statement.close();
        } catch (SQLException e) {
            System.err.println("Не удалось закрыть statement объекта core.");
        }
    }

}
