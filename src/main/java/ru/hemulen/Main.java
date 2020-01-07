package ru.hemulen;

import ru.hemulen.controller.ArchiveController;
import ru.hemulen.controller.CleanController;

import javax.swing.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
        String threshold = "";
        if (args.length != 0) {
            // Если в параметре передана конкретная дата, то значение параметра ARCHIVE_DEPTH игнорируется
            // Проверяем корректность переданной даты
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            formatter.setLenient(false); // Исключаем попытки парсера подкорректировать дату
            try {
                Date date = formatter.parse(args[0]);
                if (!formatter.format(date).equals(args[0])) {
                    System.out.println("В параметре передана некорректная дата.\nФормат даты: YYYY-MM-DD.");
                    System.exit(-1);
                }
                threshold = formatter.format(date);
            } catch (ParseException e) {
                System.out.println("В параметре передана некорректная дата.\nФормат даты: YYYY-MM-DD.");
                System.exit(-1);
            }
        }
        switch (mode) {
            case "clean":
                CleanController cleaner = new CleanController(props, threshold);
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
