package ru.hemulen.controller;

import ru.hemulen.model.DBConnector;

import java.sql.Connection;
import java.util.Properties;

/**
 * Класс предназначен для управления процессом архивирования базы адаптера
 */
public class ArchiveController {
    private DBConnector source;
    private DBConnector archive;

    public ArchiveController(Properties props) {

    }

    public void archive() {

    }

    public void close() {
        source.close();
        archive.close();
    }
}
