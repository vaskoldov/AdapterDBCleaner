package ru.hemulen.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hemulen.model.CORE;
import ru.hemulen.model.DBConnector;
import ru.hemulen.model.UI;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;

/**
 * Класс предназначен для управления процессом очистки базы адаптера
 */
public class CleanController {
    private static Logger LOG = LoggerFactory.getLogger(CleanController.class.getName());
    private DBConnector source;
    private boolean isUIPresent;

    public CleanController(Properties props, String threshold) {
        String dbName = props.getProperty("DB_PATH");
        String user = props.getProperty("USER");
        String pass = props.getProperty("PASS");
        isUIPresent = Boolean.parseBoolean(props.getProperty("IS_UI_PRESENT"));
        if (threshold.isEmpty()) {
            // Если конкретная дата не передана в параметре, то
            // вычисляем штамп времени, после которого данные останутся в базе:
            // - получаем текущее время
            Calendar currentTime = new GregorianCalendar();
            // - считываем глубину очистки (в минутах) из конфигурационного файла
            int depth = Integer.parseInt(props.getProperty("ARCHIVE_DEPTH"));
            // - вычитаем глубину очистки из текущего времени
            currentTime.add(GregorianCalendar.MINUTE, -depth);
            // - преобразуем в SQL-формат даты
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
            threshold = df.format(currentTime.getTimeInMillis());
        } else {
            threshold = threshold + " 00:00:00";
        }
        // Создаем объект для работы с базой данных
        try {
            source = new DBConnector(dbName, user, pass, isUIPresent, threshold);
        } catch (SQLException e) {
            LOG.error("Ошибка при создании экземпляра класса DBConnector.");
            LOG.error(e.getMessage());
        }

    }

    public void clean() {
        // Очищаем схему CORE
        CORE core = source.getCore();
        try {
            LOG.info("Начало очистки схемы CORE.");
            core.createTempTables();
            LOG.info("Созданы временные таблицы.");
            core.fillTempTables();
            LOG.info("Временные таблицы заполнены идентификаторами удаляемых записей.");
            core.deleteAttachmentMetadata();
            LOG.info("Удалены записи из таблицы ATTACHMENT_METADATA.");
            core.deleteMessageContent();
            LOG.info("Удалены записи из таблицы MESSAGE_CONTENT.");
            core.deleteMessageMetadata();
            LOG.info("Удалены записи из таблицы MESSAGE_METADATA.");
            core.deleteMessageState();
            LOG.info("Удалены записи из таблицы MESSAGE_STATE.");
            core.dropTempTables();
            LOG.info("Временные таблицы удалены.");
            LOG.info("Очистка схемы CORE завершена.");
        } catch (SQLException e) {
            LOG.error("Произошла ошибка при обработке данных в схеме CORE.");
            LOG.error(e.getMessage());
        }
        if (isUIPresent) {
            // Очищаем схему UI
            UI ui = source.getUI();
            try {
                LOG.info("Начало очистки схемы UI.");
                ui.createTempTables();
                LOG.info("Созданы временные таблицы.");
                ui.fillTempTables();
                LOG.info("Временные таблицы заполнены идентификаторами удаляемых записей.");
                ui.deleteBusinessAttachment();
                LOG.info("Удалены записи из таблицы BUSINESS_ATTACHMENT.");
                ui.deleteBusinessMessage();
                LOG.info("Удалены записи из таблицы BUSINESS_MESSAGE.");
                ui.deleteBusinessInteraction();
                LOG.info("Удалены записи из таблицы BUSINESS_INTERACTION.");
                ui.deleteNotification();
                LOG.info("Удалены записи из таблицы NOTIFICATION.");
                ui.dropTempTables();
                LOG.info("Временные таблицы удалены.");
                LOG.info("Очистка схемы UI завершена.");
            } catch (SQLException e) {
                LOG.error("Произошла ошибка при обработке данных в схеме UI.");
                LOG.error(e.getMessage());
            }
        }
    }

    public void close() {
        source.close();
    }
}
