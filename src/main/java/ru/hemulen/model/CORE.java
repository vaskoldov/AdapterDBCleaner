package ru.hemulen.model;

import java.sql.*;

/**
 * Класс содержит методы для манипулирования данными в схеме CORE
 */
public class CORE {
    private Connection connection;
    private Statement statement;
    private String threshold;

    public CORE(Connection connection, String threshold) throws SQLException {
        this.connection = connection;
        statement = this.connection.createStatement();
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
        // Для входящих (INCOMING) запросов существует два уровня ответов -
        // ответ ИС УВ в СМЭВ и ответ СМЭВ на отправленный ответ.
        // Наполняем REQ_TMP идентификаторами входящих запросов, полученных до threshold, на которые отправлены ответы
        // и на эти ответы пришли ответы второго уровня (на этом уровне может быть либо статус "Сообщение отправлено
        // в СМЭВ", либо ошибка отправки, поэтому статус ответов второго уровня не проверяется)
        String sql = "INSERT INTO CORE.REQ_TMP\n" +
                "SELECT DISTINCT REQ.ID\n" +
                "FROM CORE.MESSAGE_METADATA REQ\n" +
                "         LEFT JOIN CORE.MESSAGE_STATE ST ON REQ.ID = ST.ID\n" +
                "         LEFT JOIN CORE.MESSAGE_METADATA RESP1 ON REQ.ID = RESP1.REFERENCE_ID\n" +
                "         LEFT JOIN CORE.MESSAGE_METADATA RESP2 ON RESP1.ID = RESP2.REFERENCE_ID\n" +
                "WHERE REQ.MESSAGE_TYPE = 'REQUEST'\n" +
                "  AND REQ.CREATION_DATE < '" + threshold + "'\n" +
                "  AND ST.TRANSPORT_DIRECT = 'INCOMING'\n" +
                "  AND RESP1.MESSAGE_TYPE = 'RESPONSE'\n" +
                "  AND RESP2.MESSAGE_TYPE = 'RESPONSE';";
        statement.executeUpdate(sql);
        // Наполняем REQ_TMP идентификаторами исходящих запросов, отправленных до threshold,
        // на которые пришли финальные ответы
        sql = "INSERT INTO CORE.REQ_TMP\n" +
                "SELECT DISTINCT REQ.ID\n" +
                "FROM CORE.MESSAGE_METADATA REQ\n" +
                "         LEFT JOIN CORE.MESSAGE_STATE ST ON REQ.ID = ST.ID\n" +
                "         LEFT JOIN CORE.MESSAGE_METADATA RESP ON RESP.REFERENCE_ID = REQ.ID\n" +
                "         LEFT JOIN CORE.MESSAGE_CONTENT MC ON RESP.ID = MC.ID\n" +
                "WHERE REQ.MESSAGE_TYPE = 'REQUEST'\n" +
                "  AND REQ.CREATION_DATE < '" + threshold + "'\n" +
                "  AND ST.TRANSPORT_DIRECT = 'OUTGOING'\n" +
                "  AND RESP.MESSAGE_TYPE = 'RESPONSE'\n" +
                "  AND MC.MODE IN ('MESSAGE', 'REJECT', 'ERROR');";
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

    public void close(boolean defragDBOnExit, boolean compactDBOnExit) {
        if (!defragDBOnExit && !compactDBOnExit) {
            try {
                statement.close();
            } catch (SQLException e) {
                System.err.println("Не удалось закрыть statement объекта core.");
            }
        } else {
            String sql = "SHUTDOWN ";
            if (defragDBOnExit) {
                sql += "DEFRAG";
            } else {
                sql += "COMPACT";
            }
            try {
                statement.execute(sql);
            } catch (SQLException e) {
                // Ничего не делаем - здесь появится исключение "База уже закрыта"
            }
        }
    }

    public ResultSet getInteractionParticipant() throws SQLException {
        String sql = "SELECT ID, MNEMONIC, NAME, CREATION_DATE, CHANGE_DATE \n" +
                "FROM CORE.INTERACTION_PARTICIPANT; ";
        return statement.executeQuery(sql);
    }

    public void addInteractionParticipant(ResultSet records) throws SQLException {
        String sql = "INSERT INTO CORE.INTERACTION_PARTICIPANT (ID, MNEMONIC, NAME, CREATION_DATE, CHANGE_DATE) VALUES (?,?,?,?,?);";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, records.getString("ID"));
        ps.setString(2, records.getString("MNEMONIC"));
        ps.setString(3, records.getString("NAME"));
        ps.setTimestamp(4, records.getTimestamp("CREATION_DATE"));
        ps.setTimestamp(5, records.getTimestamp("CHANGE_DATE"));
        ps.executeUpdate();
        ps.close();
    }
}
