package ru.hemulen.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Класс содержит подключение к базе данных и объекты для манипулирования с данными в схемах CORE и UI
 */
public class DBConnector {
    private static Logger LOG = LoggerFactory.getLogger(DBConnector.class.getName());
    private Connection connection;
    private CORE core;
    private UI ui;

    public DBConnector(String dbName, String user, String pass, boolean isUIPresent, String threshold) throws SQLException {
        String url = String.format("jdbc:h2:file:%s;IFEXISTS=TRUE;AUTO_SERVER=TRUE", dbName);
        try {
            Class.forName("org.h2.Driver");
            connection = DriverManager.getConnection(url, user, pass);
        } catch (ClassNotFoundException e) {
            LOG.error("Не найден драйвер H2.");
            LOG.error(e.getMessage());
            System.exit(-1);
        } catch (SQLException e) {
            LOG.error(String.format("Не удалось подключиться к базе %s.", dbName));
            LOG.error(e.getMessage());
            System.exit(-1);
        }
        core = new CORE(connection, threshold);
        if (isUIPresent) {
            ui = new UI(connection, threshold);
        }
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.error("Не удалось закрыть подключение к базе данных.");
            LOG.error(e.getMessage());
        }
    }

    public CORE getCore() {
        return this.core;
    }

    public UI getUI() {
        return this.ui;
    }
}
