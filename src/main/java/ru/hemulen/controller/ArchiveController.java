package ru.hemulen.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hemulen.model.CORE;
import ru.hemulen.model.DBConnector;
import ru.hemulen.model.UI;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Properties;

/**
 * Класс предназначен для управления процессом архивирования базы адаптера
 */
public class ArchiveController {
    private static Logger LOG = LoggerFactory.getLogger(ArchiveController.class.getName());
    private DBConnector source;
    private DBConnector archive;
    private boolean isUIPresent;

    public ArchiveController(Properties props, String threshold) {
        String dbName = props.getProperty("DB_PATH");
        String arcName = props.getProperty("ARCHIVE_PATH");
        String user = props.getProperty("USER");
        String pass = props.getProperty("PASS");
        isUIPresent = Boolean.parseBoolean(props.getProperty("IS_UI_PRESENT"));
        if (threshold.trim().isEmpty()) {
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
        // Создаем объекты для работы с базами данных
        try {
            source = new DBConnector(dbName, user, pass, isUIPresent, threshold);
            archive = new DBConnector(arcName, user, pass, isUIPresent, threshold);
        } catch (SQLException e) {
            LOG.error("Ошибка при создании одного из экземпляров класса DBConnector.");
            LOG.error(e.getMessage());
        }
    }

    /**
     * Метод обновляет значения справочников в архивной базе для обеспечения ссылочной целостности,
     * а затем переносит запросы, которые были созданы до threshold и на которые получены финальные ответы,
     * из базы-источника в архивную базу.
     * Архивированные записи удаляются в базе-источнике.
     */
    public void archive() {
        LOG.info("Начало архивирования базы данных адаптера.");
        // Обновляем значения в справочниках
        updateRef();
        // Архивируем схему CORE
        archiveCORE();
        // Архивируем схему UI, если в этом есть необходимость
        if (isUIPresent) {
            archiveUI();
        }
        LOG.info("Архивирование базы данных адаптера завершено.");
    }

    public void close(boolean defragDBOnExit, boolean compactDBOnExit) {
        source.close(defragDBOnExit, compactDBOnExit);
        archive.close(defragDBOnExit, compactDBOnExit);
    }

    /**
     * Метод обновляет значения справочников в архивной базе
     */
    private void updateRef() {
        HashMap<String, String> interactionParticipant = new HashMap<>();
        CORE sourceCORE = source.getCore();
        CORE targetCORE = archive.getCore();
        try {
            // Считываем существующие в архиве значения справочника INTERACTION_PARTICIPANT
            ResultSet currentRef = targetCORE.getInteractionParticipant();
            while (currentRef.next()) {
                // Поле NAME является ключом в HashMap interactionParticipant
                interactionParticipant.put(currentRef.getString("NAME"), currentRef.getString("ID"));
            }
            // Считываем значения справочника INTERACTION_PARTICIPANT в базе-источнике
            ResultSet newRef = sourceCORE.getInteractionParticipant();
            while (newRef.next()) {
                if (!interactionParticipant.containsKey(newRef.getString(2))) {
                    // Если значения поля NAME нет в HashMap interactionParticipant,
                    // то добавляем новую запись INTERACTION_PARTICIPANT в архив
                    targetCORE.addInteractionParticipant(newRef);
                }
            }
            LOG.info("Обработан справочник INTERACTION_PARTICIPANT схемы CORE.");
        } catch (SQLException e) {
            LOG.error("Ошибка при обработке справочника InteractionParticipant.");
            LOG.error(e.getMessage());
        } finally {
            sourceCORE.close(false, false);
            targetCORE.close(false, false);
        }
        if (isUIPresent) {
            // Обновляем значения справочников:
            // - INQUIRY_VERSION
            //TODO: Добавить обработку файлов настроек для INQUIRY_VERSION
             // - INFORMATION_SYSTEM
            // - ROLE
            // - USER
            // в схеме UI
            HashMap<String, String> inquiryVersion = new HashMap<>();
            HashMap<String, String> informationSystem = new HashMap<>();
            HashMap<String, String> role = new HashMap<>();
            HashMap<String, String> user = new HashMap<>();
            UI sourceUI = source.getUI();
            UI targetUI = archive.getUI();
            try {
                // Считываем существующие в архиве значения справочника INQUIRY_VERSION
                ResultSet currentRef = targetUI.getInquiryVersion();
                while (currentRef.next()) {
                    inquiryVersion.put(currentRef.getString("NAMESPACE"), currentRef.getString("ID"));
                }
                // Считываем значения справочника INQUIRY_VERSION в базе-источнике
                ResultSet newRef = sourceUI.getInquiryVersion();
                while (newRef.next()) {
                    if (!inquiryVersion.containsKey(newRef.getString("NAMESPACE"))) {
                        targetUI.addInquiryVersion(newRef);
                    }
                }
                LOG.info("Обработан справочник INQUIRY_VERSION схемы UI.");
            } catch (SQLException e) {
                LOG.error("Ошибка при обработке справочника INQUIRY_VERSION.");
                LOG.error(e.getMessage());
            }
            try {
                // Считываем существующие в архиве значения справочника INFORMATION_SYSTEM
                ResultSet currentRef = targetUI.getInformationSystem();
                while (currentRef.next()) {
                    informationSystem.put(currentRef.getString("MNEMONIC"), currentRef.getString("ID"));
                }
                // Считываем значения справочника INFORMATION_SYSTEM в базе-источнике
                ResultSet newRef = sourceUI.getInformationSystem();
                while (newRef.next()) {
                    if (!informationSystem.containsKey(newRef.getString("MNEMONIC"))) {
                        targetUI.addInformationSystem(newRef);
                    }
                }
                LOG.info("Обработан справочник INFORMATION_SYSTEM схемы UI.");
            } catch (SQLException e) {
                LOG.error("Ошибка при обработке справочника INFORMATION_SYSTEM.");
                LOG.error(e.getMessage());
            }
            try {
                // Считываем существующие в архиве значения справочника ROLE
                ResultSet currentRef = targetUI.getRole();
                while (currentRef.next()) {
                    role.put(currentRef.getString("NAME"), currentRef.getString("ID"));
                }
                // Считываем значения справочника ROLE в базе-источнике
                ResultSet newRef = sourceUI.getRole();
                while (newRef.next()) {
                    if (!role.containsKey(newRef.getString("NAME"))) {
                        targetUI.addRole(newRef);
                    }
                }
                LOG.info("Обработан справочник ROLE схемы UI.");
            } catch (SQLException e) {
                LOG.error("Ошибка при обработке справочника ROLE.");
                LOG.error(e.getMessage());
            }
            try {
                // Считываем существующие в архиве значения справочника USER
                ResultSet currentRef = targetUI.getUser();
                while (currentRef.next()) {
                    user.put(currentRef.getString("NAME"), currentRef.getString("ID"));
                }
                // Считываем значения справочника USER в базе-источнике
                ResultSet newRef = sourceUI.getUser();
                while (newRef.next()) {
                    if (!user.containsKey(newRef.getString("NAME"))) {
                        targetUI.addUser(newRef);
                    }
                }
                LOG.info("Обработан справочник USER схемы UI.");
            } catch (SQLException e) {
                LOG.error("Ошибка при обработке справочника USER.");
                LOG.error(e.getMessage());
            }
        }
    }

    /**
     * Метод переносит запросы, которые были созданы до threshold и на которые получены финальные ответы,
     * из схемы CORE базы-источника в схему CORE архивной базы.
     * Архивированные записи удаляются из схемы CORE базы-источника.
     */
    private void archiveCORE() {

    }

    /**
     * Метод переносит запросы, которые были созданы до threshold и на которые получены финальные ответы,
     * из схемы UI базы-источника в схему UI архивной базы.
     * Архивированные записи удаляются из схемы UI базы-источника.
     */
    private void archiveUI() {

    }
}
