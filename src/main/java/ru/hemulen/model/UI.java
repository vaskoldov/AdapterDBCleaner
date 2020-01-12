package ru.hemulen.model;

import java.sql.*;

/**
 * Класс содержит методы для манипулирования данными в схеме UI
 */
public class UI {
    private Connection connection;
    private Statement statement;
    private String threshold;

    public UI(Connection connection, String threshold) throws SQLException {
        this.connection = connection;
        statement = this.connection.createStatement();
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

    public ResultSet getInquiryVersion() throws SQLException {
        String sql = "SELECT ID, \n" +
                "       SCHEMA_FILE_ID, \n" +
                "       SCHEMA_LOCATION, \n" +
                "       NAMESPACE, \n" +
                "       DESCRIPTION, \n" +
                "       VERSION, \n" +
                "       TYPE, \n" +
                "       OWNER, \n" +
                "       APPLIED_SETTINGS_FILE_ID, \n" +
                "       INTERACTION_TYPE, \n" +
                "       TEST_MESSAGE, \n" +
                "       CREATION_DATE, \n" +
                "       CHANGE_DATE \n" +
                "FROM UI.INQUIRY_VERSION;";
        return statement.executeQuery(sql);
    }

    public void addInquiryVersion(ResultSet record) throws SQLException {
        String sql = "INSERT INTO UI.INQUIRY_VERSION (\n" +
                "    ID, \n" +
                "    SCHEMA_FILE_ID, \n" +
                "    SCHEMA_LOCATION, \n" +
                "    NAMESPACE, \n" +
                "    DESCRIPTION, \n" +
                "    VERSION, \n" +
                "    TYPE, \n" +
                "    OWNER, \n" +
                "    APPLIED_SETTINGS_FILE_ID, \n" +
                "    INTERACTION_TYPE, \n" +
                "    TEST_MESSAGE, \n" +
                "    CREATION_DATE, \n" +
                "    CHANGE_DATE\n" +
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);\n";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, record.getString("ID"));
        ps.setString(2, record.getString("SCHEMA_FILE_ID"));
        ps.setString(3, record.getString("SCHEMA_LOCATION"));
        ps.setString(4, record.getString("NAMESPACE"));
        ps.setString(5, record.getString("DESCRIPTION"));
        ps.setString(6, record.getString("VERSION"));
        ps.setString(7, record.getString("TYPE"));
        ps.setString(8, record.getString("OWNER"));
        ps.setString(9, record.getString("APPLIED_SETTINGS_FILE_ID"));
        ps.setString(10, record.getString("INTERACTION_TYPE"));
        ps.setBoolean(11, record.getBoolean("TEST_MESSAGE"));
        ps.setTimestamp(12, record.getTimestamp("CREATION_DATE"));
        ps.setTimestamp(13, record.getTimestamp("CHANGE_DATE"));
        ps.executeUpdate();
    }

    public ResultSet getInformationSystem() throws SQLException {
        String sql = "SELECT ID, MNEMONIC, DESCRIPTION, ATTACHMENT_PATH FROM UI.INFORMATION_SYSTEM;";
        return statement.executeQuery(sql);
    }

    public void addInformationSystem(ResultSet record) throws SQLException {
        String sql = "INSERT INTO UI.INFORMATION_SYSTEM (ID, MNEMONIC, DESCRIPTION, ATTACHMENT_PATH) VALUES (?,?,?,?);";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, record.getString("ID"));
        ps.setString(2, record.getString("MNEMONIC"));
        ps.setString(3, record.getString("DESCRIPTION"));
        ps.setString(4, record.getString("ATTACHMENT_PATH"));
        ps.executeUpdate();
    }

    public ResultSet getRole() throws SQLException {
        String sql = "SELECT ID, NAME, DESCRIPTION FROM UI.ROLE;";
        return statement.executeQuery(sql);
    }

    public void addRole(ResultSet record) throws SQLException {
        String sql = "INSERT INTO UI.ROLE (ID, NAME, DESCRIPTION) VALUES (?,?,?);";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, record.getString("ID"));
        ps.setString(2, record.getString("NAME"));
        ps.setString(3, record.getString("DESCRIPTION"));
        ps.executeUpdate();
    }

    public ResultSet getUser() throws SQLException {
        String sql = "SELECT ID, \n" +
                "       ROLE_ID, \n" +
                "       NAME, \n" +
                "       FULL_NAME, \n" +
                "       PASSWORD, \n" +
                "       KEYSTORE_ALIAS, \n" +
                "       KEYSTORE_PASSWORD, \n" +
                "       DISCONTINUE_DATE, \n" +
                "       BLOCKED, \n" +
                "       CREATION_DATE, \n" +
                "       CHANGE_DATE \n" +
                "FROM UI.USER;";
        return statement.executeQuery(sql);
    }

    public void addUser(ResultSet record) throws SQLException {
        String sql = "INSERT INTO UI.USER (\n" +
                "    ID, \n" +
                "    ROLE_ID, \n" +
                "    NAME, \n" +
                "    FULL_NAME, \n" +
                "    PASSWORD, \n" +
                "    KEYSTORE_ALIAS, \n" +
                "    KEYSTORE_PASSWORD, \n" +
                "    DISCONTINUE_DATE, \n" +
                "    BLOCKED, \n" +
                "    CREATION_DATE, \n" +
                "    CHANGE_DATE\n" +
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?);";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, record.getString("ID"));
        ps.setString(2, record.getString("ROLE_ID"));
        ps.setString(3, record.getString("NAME"));
        ps.setString(4, record.getString("FULL_NAME"));
        ps.setString(5, record.getString("PASSWORD"));
        ps.setString(6, record.getString("KEYSTORE_ALIAS"));
        ps.setString(7, record.getString("KEYSTORE_PASSWORD"));
        ps.setTimestamp(8, record.getTimestamp("DISCONTINUE_DATE"));
        ps.setBoolean(9, record.getBoolean("BLOCKED"));
        ps.setTimestamp(10, record.getTimestamp("CREATION_DATE"));
        ps.setTimestamp(11, record.getTimestamp("CHANGE_DATE"));
        ps.executeUpdate();
    }
}
