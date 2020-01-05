package ru.hemulen;

import ru.hemulen.controller.ArchiveController;
import ru.hemulen.controller.CleanController;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Main {
    private static final String configName = "./config/config.ini";
    private static String mode;

    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(new File(configName)));
        } catch (IOException e) {
            System.out.println("Не удалось загрузить файл конфигурации.");
            e.printStackTrace();
            System.exit(-1);
        }
        if (props.getProperty("ARCHIVE_PATH") != null && props.getProperty("ARCHIVE_PATH").trim().isEmpty()) {
            mode = "clean";     // Если не указан путь к базе с архивом, то выполняется очистка текущей базы
        } else {
            mode = "archive";   // Иначе выполняется архивирование в указанную базу
        }
        switch (mode) {
            case "clean":
                CleanController cleaner = new CleanController(props);
                cleaner.clean();
                cleaner.close();
                break;
            case "archive":
                ArchiveController archiver = new ArchiveController(props);
                archiver.archive();
                archiver.close();
                break;
        }
    }
}
