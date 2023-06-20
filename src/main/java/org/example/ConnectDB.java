package org.example;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.valueOf;

public class ConnectDB {
    private static Connection connection;

    // --------ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ--------
    private static void connectBD() throws ClassNotFoundException, SQLException {
        connection = null;
        Class.forName("org.sqlite.JDBC", true, Thread.currentThread().getContextClassLoader());
        connection = DriverManager.getConnection("jdbc:sqlite:dp_barbershop_bd.db");
        System.out.println("База Подключена!");
    }



    // ----------------GET QUERIES----------------
    public static List<String> getRecordsForDate(String date) throws SQLException, ClassNotFoundException {
        connectBD();

        System.out.println("date= |" + date + "|");

        List<String> result = new ArrayList<>();
        String query = "SELECT t1.time as starttime, t2.time as endtime, cName.name as clientName, telephone_client,\n" +
                "name_procedure, mName.name as masterName, r.price as price\n" +
                "FROM record r\n" +
                "INNER JOIN master m on r.id_master = m.id_master\n" +
                "INNER JOIN name mName on m.id_name = mName.id_name\n" +
                "INNER JOIN client c on r.id_client = c.id_client\n" +
                "INNER JOIN name cName on c.id_name= cName.id_name\n" +
                "INNER JOIN procedure p on r.id_procedure = p.id_procedure\n" +
                "INNER JOIN time t1 on r.id_starttime = t1.id_time\n" +
                "INNER JOIN time t2 on r.id_endtime = t2.id_time\n" +
                "WHERE r.date = ?\n" +
                "ORDER BY t1.time ";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, date);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("starttime"));
                    result.add(resultSet.getString("endtime"));
                    result.add(resultSet.getString("clientName"));
                    result.add(resultSet.getString("telephone_client"));
                    result.add(resultSet.getString("name_procedure"));
                    result.add(resultSet.getString("masterName"));
                    result.add(resultSet.getString("price"));
                    System.out.println( "starttime = " + resultSet.getString("starttime"));
                    System.out.println( "endtime = " + resultSet.getString("endtime"));
                    System.out.println( "clientName = " + resultSet.getString("clientName"));
                    System.out.println( "telephone_client = " + resultSet.getString("telephone_client"));
                    System.out.println( "procedure = " + resultSet.getString("name_procedure"));
                    System.out.println( "masterName = " + resultSet.getString("masterName"));
                    System.out.println( "price = " + resultSet.getString("price"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> getRecordsForDateMaster(String date, String master) throws SQLException, ClassNotFoundException {
        connectBD();

        System.out.println("date= |" + date + "|");

        String nameId = getIdName(master);
        String masterId = getIdMaster(nameId);
        System.out.println("masterId= |" + masterId + "|");

        List<String> result = new ArrayList<>();
        String query = "SELECT t1.time as starttime, t2.time as endtime, cName.name as clientName, telephone_client,\n" +
                "name_procedure, r.price as price\n" +
                "FROM record r\n" +
                "INNER JOIN master m on r.id_master = m.id_master\n" +
                "INNER JOIN client c on r.id_client = c.id_client\n" +
                "INNER JOIN name cName on c.id_name= cName.id_name\n" +
                "INNER JOIN procedure p on r.id_procedure = p.id_procedure\n" +
                "INNER JOIN time t1 on r.id_starttime = t1.id_time\n" +
                "INNER JOIN time t2 on r.id_endtime = t2.id_time\n" +
                "WHERE r.date = ? AND m.id_master = ?\n" +
                "ORDER BY t1.time DESC ";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, date);
            statement.setString(2, masterId);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("starttime"));
                    result.add(resultSet.getString("endtime"));
                    result.add(resultSet.getString("clientName"));
                    result.add(resultSet.getString("telephone_client"));
                    result.add(resultSet.getString("name_procedure"));
                    result.add(resultSet.getString("price"));
                    System.out.println( "starttime = " + resultSet.getString("starttime"));
                    System.out.println( "endtime = " + resultSet.getString("endtime"));
                    System.out.println( "clientName = " + resultSet.getString("clientName"));
                    System.out.println( "telephone_client = " + resultSet.getString("telephone_client"));
                    System.out.println( "procedure = " + resultSet.getString("name_procedure"));
                    System.out.println( "price = " + resultSet.getString("price"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> getRecordIntervalsForDateMasterRange(String date, String master, String rangeString) throws SQLException, ClassNotFoundException {
        connectBD();

        int range = Integer.parseInt(rangeString);
        System.out.println("range= |" + range + "|");

        String idMasterName = getIdName(master);
        String idMaster = getIdMaster(idMasterName);
        System.out.println("idMaster= |" + idMaster + "|");

        List<String> result = new ArrayList<>();
        String query = "WITH busy_times AS ( " +
                "    SELECT r.id_starttime, r.id_endtime " +
                "    FROM record r " +
                "    WHERE r.date = ? AND r.id_master = ? " +
                ") " +
                "SELECT t1.time as start_time, t2.time as end_time " +
                "FROM time t1 " +
                "JOIN time t2 ON t2.id_time = t1.id_time + ? " +
                "WHERE NOT EXISTS ( " +
                "    SELECT 1 " +
                "    FROM busy_times bt " +
                "    WHERE bt.id_endtime > t1.id_time AND bt.id_starttime < t2.id_time " +
                ") ";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, date);
            statement.setString(2, idMaster);
            statement.setInt(3, range);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("start_time"));
                    result.add(resultSet.getString("end_time"));
                    System.out.println( "start_time = " + resultSet.getString("start_time"));
                    System.out.println( "end_time = " + resultSet.getString("end_time"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> getAllMasters() throws SQLException, ClassNotFoundException {
        connectBD();
        List<String> result = new ArrayList<>();
        String query = "SELECT name, telephone_master, password " +
                "FROM master m " +
                "inner join name n on m.id_name = n.id_name";

        try (PreparedStatement statement = connection.prepareStatement(query)) {

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(valueOf(resultSet.getString("name")));
                    result.add(valueOf(resultSet.getString("telephone_master")));
                    result.add(valueOf(resultSet.getString("password")));
                    System.out.println( "name = " + resultSet.getString("name"));
                    System.out.println( "telephone_master = " + resultSet.getString("telephone_master"));
                    System.out.println( "password = " + resultSet.getString("password"));
                }
                System.out.println("Таблица выведена");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> getMastersForDayAndRange(String date, String rangeString) throws SQLException, ClassNotFoundException {
        connectBD();

        int range = Integer.parseInt(rangeString);
        System.out.println("range= |" + range + "|");
        System.out.println("date= |" + date + "|");

        List<String> result = new ArrayList<>();
        String query = "SELECT name as name, telephone_master, password \n" +
                "FROM master m\n" +
                "INNER JOIN name n ON n.id_name = m.id_name\n" +
                "WHERE EXISTS \n" +
                "(\n" +
                "\tWITH busy_times AS (\n" +
                "\t\tSELECT r.id_starttime, r.id_endtime\n" +
                "\t\tFROM record r\n" +
                "\t\tWHERE r.date = ? AND r.id_master = m.id_master\n" +
                "\t)\n" +
                "\tSELECT t1.time as start_time, t2.time as end_time\n" +
                "\tFROM time t1\n" +
                "\tJOIN time t2 ON t2.id_time = t1.id_time + ?\n" +
                "\tWHERE NOT EXISTS (\n" +
                "\t\tSELECT 1\n" +
                "\t\tFROM busy_times bt\n" +
                "\t\tWHERE bt.id_endtime > t1.id_time AND bt.id_starttime < t2.id_time\n" +
                "\t)\n" +
                ")";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, date);
            statement.setInt(2, range);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(valueOf(resultSet.getString("name")));
                    result.add(valueOf(resultSet.getString("telephone_master")));
                    result.add(valueOf(resultSet.getString("password")));
                    System.out.println( "name = " + resultSet.getString("name"));
                    System.out.println( "telephone_master = " + resultSet.getString("telephone_master"));
                    System.out.println( "password = " + resultSet.getString("password"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> getWorkingMasters(String date) throws SQLException, ClassNotFoundException {
        connectBD();
        List<String> result = new ArrayList<>();
        String query = "SELECT name as name, telephone_master, password " +
                "FROM master m " +
                "INNER JOIN name n ON m.id_name = n.id_name " +
                "WHERE EXISTS ( " +
                "SELECT * FROM record r " +
                "WHERE r.id_master = m.id_master AND r.date == ? );";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, date);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(valueOf(resultSet.getString("name")));
                    result.add(valueOf(resultSet.getString("telephone_master")));
                    result.add(valueOf(resultSet.getString("password")));
                    System.out.println( "name = " + resultSet.getString("name"));
                    System.out.println( "telephone_master = " + resultSet.getString("telephone_master"));
                    System.out.println( "password = " + resultSet.getString("password"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> getAllTime() throws SQLException, ClassNotFoundException {
        connectBD();
        List<String> result = new ArrayList<>();
        String query = "SELECT time FROM time";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("time"));
                    System.out.println( "time = " + resultSet.getString("time"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> checkUser(String login, String password) throws SQLException, ClassNotFoundException {
        connectBD();
        List<String> result = new ArrayList<>();
        String query = "SELECT COUNT(*) AS count FROM users WHERE login = ? AND password = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, login);
            statement.setString(2, password);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    result.add(valueOf(resultSet.getInt("count") > 0));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> checkMaster(String master, String password) throws SQLException, ClassNotFoundException {
        connectBD();
        List<String> result = new ArrayList<>();
        String query = "SELECT COUNT(*) AS count " +
                "FROM master m " +
                "INNER JOIN name n ON n.id_name = m.id_name " +
                "WHERE name = ? AND password = ? ";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, master);
            statement.setString(2, password);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()){
                    result.add(valueOf(resultSet.getInt("count") > 0));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> getAllProcedure() throws SQLException, ClassNotFoundException {
        connectBD();
        List<String> result = new ArrayList<>();
        String query = "SELECT name_procedure, price FROM procedure";

        try (PreparedStatement statement = connection.prepareStatement(query)) {

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(valueOf(resultSet.getString("name_procedure")));
                    result.add(valueOf(resultSet.getString("price")));
                    System.out.println( "name_procedure = " + resultSet.getString("name_procedure"));
                    System.out.println( "price = " + resultSet.getString("price"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> getReport(String master, String procedure, String fromDate, String untilDate) throws SQLException, ClassNotFoundException {
        connectBD();
        List<String> result = new ArrayList<>();
        String query = "SELECT n.name, name_procedure, count() AS count, p.price, " +
                "count() * r.price AS cost " +
                "FROM record r " +
                "inner join master m on r.id_master = m.id_master " +
                "inner join name n on m.id_name = n.id_name " +
                "inner join procedure p on r.id_procedure = p.id_procedure ";

        boolean isMasterValid = master != null && !master.isBlank() && !master.equals("null");
        boolean isProcedureValid = procedure != null && !procedure.isBlank() && !procedure.equals("null");
        boolean isFromDateValid = fromDate != null && !fromDate.isBlank() && !fromDate.equals("null");
        boolean isUntilDateValid = untilDate != null && !untilDate.isBlank() && !untilDate.equals("null");

        List<String> clauses = new ArrayList<>();
        if (isMasterValid) {
            clauses.add("name = ?");
        }
        if (isProcedureValid) {
            clauses.add("name_procedure = ?");
        }
        if (isFromDateValid) {
            clauses.add("date > ?");
        }
        if (isUntilDateValid) {
            clauses.add("date < ?");
        }
        if (!clauses.isEmpty()) {
            query += " WHERE " + String.join(" AND ", clauses);
        }
        query += " GROUP BY name, name_procedure";
        System.out.println(query);

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            int index = 1;
            if (isMasterValid) {
                statement.setString(index++, master);
            }
            if (isProcedureValid) {
                statement.setString(index++, procedure);
            }
            if (isFromDateValid) {
                statement.setString(index++, fromDate);
            }
            if (isUntilDateValid) {
                statement.setString(index++, untilDate);
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("name"));
                    result.add(resultSet.getString("name_procedure"));
                    result.add(resultSet.getString("count"));
                    result.add(resultSet.getString("price"));
                    result.add(resultSet.getString("cost"));
                    System.out.println( "name = " + resultSet.getString("name"));
                    System.out.println( "name_procedure = " + resultSet.getString("name_procedure"));
                    System.out.println( "count = " + resultSet.getString("count"));
                    System.out.println( "price = " + resultSet.getString("price"));
                    System.out.println( "cost = " + resultSet.getString("cost"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }




    // ----------------GET ID QUERIES----------------

    private static String getIdProcedure(String procedure) {
        String query = "SELECT id_procedure From procedure where name_procedure == ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, procedure);

            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? String.valueOf(resultSet.getString("id_procedure")) : null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getIdClient(String idNameClient, String phone) {
        String query = "SELECT id_client From client where id_name == ? AND telephone_client == ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, idNameClient);
            statement.setString(2, phone);

            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? String.valueOf(resultSet.getString("id_client")) : null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getIdMaster(String idNameMaster) {
        String query = "SELECT id_master From master where id_name == ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, idNameMaster);

            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? String.valueOf(resultSet.getString("id_master")) : null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getIdTime(String time) {
        String query = "SELECT id_time From time where time == ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, time);

            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? String.valueOf(resultSet.getInt("id_time")) : null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getIdName(String name) {
        String query = "SELECT id_name FROM name where name == ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, name);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("id_name");
                } else {
                    createName(name);
                    try (ResultSet resultSet1 = statement.executeQuery()) {
                        return resultSet1.getString("id_name");
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }



    // ----------------ADD QUERIES----------------

    public static void createMaster(String name, String telephone, String password) throws SQLException, ClassNotFoundException {
        connectBD();
        String idName = getIdName(name);

        String query = "INSERT INTO master(id_name, telephone_master, password) VALUES(?, ?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, idName);
            statement.setString(2, telephone);
            statement.setString(3, password);

            statement.executeUpdate();
            System.out.println("Создан новый мастер");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
    }

    public static void addRecord(String date, String starttime, String endtime, String masterName,
                                 String clientName, String clientPhone, String procedure, String price) throws SQLException, ClassNotFoundException {
        connectBD();
        String idStartTime = getIdTime(starttime);
        System.out.println("idStartTime = |" +  idStartTime + "|");
        String idEndTime = getIdTime(endtime);
        System.out.println("idEndTime = |" +  idEndTime + "|");
        String idNameMaster = getIdName(masterName);
        System.out.println("idNameMaster = |" +  idNameMaster + "|");
        String idMaster = getIdMaster(idNameMaster);
        System.out.println("idMaster = |" +  idMaster + "|");

        String idNameCLient = getIdName(clientName);
        System.out.println("idNameClient = |" +  idNameCLient + "|");

        String idClient = getIdClient(idNameCLient, clientPhone);
        if(idClient == null) {
            createClient(idNameCLient, clientPhone);
            idClient = getIdClient(idNameCLient, clientPhone);
        }
        System.out.println("idClient = |" +  idClient + "|");

        String idProcedure = getIdProcedure(procedure);
        System.out.println("idProcedure = |" +  idProcedure + "|");

        String query = "INSERT INTO record (date, id_master, id_client, id_procedure, id_starttime, id_endtime, price) VALUES (?, ?, ?, ?, ?, ?, ?);";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, date);
            statement.setString(2, idMaster);
            statement.setString(3, idClient);
            statement.setString(4, idProcedure);
            statement.setString(5, idStartTime);
            statement.setString(6, idEndTime);
            statement.setString(7, price);

            statement.executeUpdate();
            System.out.println("Создана новая запись");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
    }

    private static void createClient(String idName, String telephone) {
        String query = "INSERT INTO client(id_name, telephone_client) VALUES(?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, idName);
            statement.setString(2, telephone);

            statement.executeUpdate();
            System.out.println("Создан новый клиент");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createName(String name) {
        String query = "INSERT INTO name(name) VALUES(?)";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, name);

            statement.executeUpdate();
            System.out.println("Создано новое имя");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void addHistory(String history)throws SQLException, ClassNotFoundException {
        connectBD();
        LocalDate localDate = LocalDate.now();
        String localDateString = localDate.format(DateTimeFormatter.ofPattern("dd.MM.yyyy"));

        String query = "INSERT INTO history(history, date) VALUES(?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, history);
            statement.setString(2, localDateString);

            statement.executeUpdate();
            System.out.println("Добавлена история");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
    }



    // ----------------UPDATE QUERIES----------------

    public static void editRecord(String date, String starttime, String masterName, String clientName, String telephone) throws SQLException, ClassNotFoundException {
        connectBD();
        String idTime = getIdTime(starttime); // id времени

        String idNameMaster = getIdName(masterName);
        String idMaster = getIdMaster(idNameMaster); // id мастера

        String idNameCLient = getIdName(clientName);

        String idClient = getIdClient(idNameCLient, telephone); // id клиент
        if(idClient == null) {
            createClient(idNameCLient, telephone);
            idClient = getIdClient(idNameCLient, telephone);
        }

        String query = "UPDATE record SET id_client = ? WHERE date == ? AND id_starttime == ? AND id_master = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, idClient);
            statement.setString(2, date);
            statement.setString(3, idTime);
            statement.setString(4, idMaster);

            statement.executeUpdate();
            System.out.println("Запись обновлена");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
    }

    public static void moveRecord(String oldDate, String oldStartTime, String oldEndTime, String masterName,
                                  String newDate,  String newStartTime,  String newEndTime) throws SQLException, ClassNotFoundException {
        connectBD();

        String idStartTimeOld = getIdTime(oldStartTime);
        System.out.println("idStartTimeOld = |" +  idStartTimeOld + "|");
        String idEndTimeOld = getIdTime(oldEndTime);
        System.out.println("idEndTimeOld = |" +  idEndTimeOld + "|");
        String idNameMaster = getIdName(masterName);
        System.out.println("idNameMaster = |" +  idNameMaster + "|");
        String idMaster = getIdMaster(idNameMaster);
        System.out.println("idMaster = |" +  idMaster + "|");

        String idStartTimeNew = getIdTime(newStartTime);
        System.out.println("idStartTimeNew = |" +  idStartTimeNew + "|");
        String idEndTimeNew = getIdTime(newEndTime);
        System.out.println("idEndTimeNew = |" +  idEndTimeNew + "|");

        String query = "UPDATE record SET date = ?, id_starttime = ?, id_endtime = ? " +
                "WHERE date == ? AND id_starttime == ? AND id_endtime == ? AND id_master = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, newDate);
            statement.setString(2, idStartTimeNew);
            statement.setString(3, idEndTimeNew);
            statement.setString(4, oldDate);
            statement.setString(5, idStartTimeOld);
            statement.setString(6, idEndTimeOld);
            statement.setString(7, idMaster);

            statement.executeUpdate();
            System.out.println("Запись передвинута");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
    }

    public static void updateMaster(String nameOld, String telephoneOld, String passwordOld, String nameNew, String telephoneNew, String passwordNew) throws SQLException, ClassNotFoundException {
        connectBD();
        String idNameOld = getIdName(nameOld);
        String idNameNew = getIdName(nameNew);

        String query = "UPDATE master SET id_name = ?, telephone_master = ?, password = ? " +
                "WHERE id_name = ? AND telephone_master = ? AND password = ? ";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, idNameNew);
            statement.setString(2, telephoneNew);
            statement.setString(3, passwordNew);
            statement.setString(4, idNameOld);
            statement.setString(5, telephoneOld);
            statement.setString(6, passwordOld);

            statement.executeUpdate();
            System.out.println("Создан новый мастер");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
    }



    // ----------------DELETE QUERIES----------------

    public static void deleteMaster(String name, String telephone) throws SQLException, ClassNotFoundException {
        connectBD();
        String idName = getIdName(name);

        String query = "DELETE FROM master where id_name=? and telephone_master=?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, idName);
            statement.setString(2, telephone);

            statement.executeUpdate();
            System.out.println("Мастер удалён");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
    }

    public static void deleteRecord(String date, String starttime,  String master) throws SQLException, ClassNotFoundException {
        connectBD();

        String idNameMaster = getIdName(master);
        String idMaster = getIdMaster(idNameMaster);
        String idTime = getIdTime(starttime);

        String query = "DELETE FROM record where date = ? AND id_starttime = ? AND id_master = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, date);
            statement.setString(2, idTime);
            statement.setString(3, idMaster);

            statement.executeUpdate();
            System.out.println("Запись удалёна");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
    }




    // -------------------OTHERS QUERIES---------------------------
    public static List<String> getTimeForDay(String date) throws SQLException, ClassNotFoundException {
        connectBD();
        List<String> result = new ArrayList<>();
        String query = "SELECT DISTINCT time.time " +
                "FROM time " +
                "CROSS JOIN master " +
                "LEFT JOIN record ON record.id_time = time.id_time AND record.date = ? " +
                "AND record.id_master = master.id_master\n" +
                "WHERE record.id_record IS NULL " +
                "GROUP BY time.id_time, master.id_master " +
                "HAVING COUNT(record.id_record) = 0;";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, date);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(valueOf(resultSet.getString("time")));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

    public static List<String> getHistory() throws SQLException, ClassNotFoundException {
        connectBD();
        List<String> result = new ArrayList<>();
        String query = "SELECT date, history FROM history";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("date"));
                    result.add(resultSet.getString("history"));
                    System.out.println( "date = " + resultSet.getString("date"));
                    System.out.println( "history = " + resultSet.getString("history"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connection.close();
        return result;
    }

}
