package org.example;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MonoThreadClientHandler implements Runnable{
    private Socket client;
    public MonoThreadClientHandler(Socket client) {
        this.client = client;
    }

    @Override
    public void run() {
        try {
            // инициируем каналы общения в сокете, для сервера

            // канал записи в сокет следует инициализировать сначала канал чтения для избежания блокировки выполнения программы на ожидании заголовка в сокете
            DataOutputStream out = new DataOutputStream(client.getOutputStream());
            DataInputStream in = new DataInputStream(client.getInputStream());

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // основная рабочая часть //
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            // начинаем диалог с подключенным клиентом в цикле, пока сокет не
            // закрыт клиентом
            while (!client.isClosed()) {
                System.out.println("Server reading from channel");

                // серверная нить ждёт в канале чтения (inputstream) получения
                // данных клиента после получения данных считывает их
                String entry = in.readUTF();
                System.out.println("entry" + entry);

                List<String> reqList = new ArrayList<>(Arrays.asList(entry.split("\\|")));
                System.out.println("reqList.get(0) = " + reqList.get(0));
                if(reqList.get(0) != null){
                    List<String> resultStrings = new ArrayList<>();
                    String history = "";
                    if(reqList.get(0).equals(RequestServer.REQUESR_GET_RECORDS_FOR_DAY.getNameRequest())){
                        System.out.println("REQUESR_GET_RECORDS_FOR_DAY");
                        resultStrings = ConnectDB.getRecordsForDate(reqList.get(1));
                    } else if (reqList.get(0).equals(RequestServer.REQUESR_GET_RECORDS_FOR_DAY_MASTER.getNameRequest())) {
                        System.out.println("REQUESR_GET_RECORDS_FOR_DAY_MASTER");
                        resultStrings = ConnectDB.getRecordsForDateMaster(reqList.get(1), reqList.get(2));
                    } else if (reqList.get(0).equals(RequestServer.REQUESR_GET_RECORDS_FOR_DATE_MASTER_RANGE.getNameRequest())) {
                        System.out.println("REQUESR_GET_RECORDS_FOR_DATE_MASTER_RANGE");
                        resultStrings = ConnectDB.getRecordIntervalsForDateMasterRange(reqList.get(1), reqList.get(2), reqList.get(3));
                    } else if (reqList.get(0).equals(RequestServer.REQUESR_GET_ALL_MASTERS.getNameRequest())) {
                        System.out.println("REQUESR_GET_ALL_MASTERS");
                        resultStrings = ConnectDB.getAllMasters();
                    } else if (reqList.get(0).equals(RequestServer.REQUESR_GET_MASTERS_FOR_DAY_RANGE.getNameRequest())) {
                        System.out.println("REQUESR_GET_MASTERS_FOR_DAY_RANGE");
                        resultStrings = ConnectDB.getMastersForDayAndRange(reqList.get(1), reqList.get(2));
                    } else if (reqList.get(0).equals(RequestServer.REQUESR_GET_WORKING_MASTERS_FOR_DAY.getNameRequest())) {
                        System.out.println("REQUESR_GET_WORKING_MASTERS_FOR_DAY");
                        resultStrings = ConnectDB.getWorkingMasters(reqList.get(1));
                    } else if (reqList.get(0).equals(RequestServer.REQUESR_GET_TIME_FOR_DAY.getNameRequest())) {
                        System.out.println("REQUESR_GET_TIME_FOR_DAY");
                        resultStrings = ConnectDB.getTimeForDay(reqList.get(1));
                    }else if (reqList.get(0).equals(RequestServer.REQUESR_GET_ALL_TIME.getNameRequest())) {
                        System.out.println("REQUESR_GET_ALL_TIME");
                        resultStrings = ConnectDB.getAllTime();
                    }else if (reqList.get(0).equals(RequestServer.REQUESR_CHECK_USER.getNameRequest())) {
                        System.out.println("REQUESR_CHECK_USER");
                        resultStrings = ConnectDB.checkUser(reqList.get(1), reqList.get(2));
                    }else if (reqList.get(0).equals(RequestServer.REQUESR_CHECK_MASTER.getNameRequest())) {
                        System.out.println("REQUESR_CHECK_MASTER");
                        resultStrings = ConnectDB.checkMaster(reqList.get(1), reqList.get(2));
                    }else if (reqList.get(0).equals(RequestServer.REQUESR_ADD_MASTER.getNameRequest())) {
                        System.out.println("REQUESR_ADD_MASTER");
                        ConnectDB.createMaster(reqList.get(1), reqList.get(2), reqList.get(3));
                        history = "Добавили мастера: " + reqList.get(1) + ", с телефоном :" + reqList.get(2);
                    } else if (reqList.get(0).equals(RequestServer.REQUESR_UPDATE_MASTER.getNameRequest())) {
                        System.out.println("REQUESR_UPDATE_MASTER");
                        ConnectDB.updateMaster(reqList.get(1), reqList.get(2), reqList.get(3), reqList.get(4), reqList.get(5), reqList.get(6));
                        history = "Обновили мастера: " + reqList.get(1) + ", с телефоном :" + reqList.get(2) + "\n" +
                                "Новые значение: имя: " + reqList.get(4) + ", телефон :" + reqList.get(5);
                    }else if(reqList.get(0).equals(RequestServer.REQUESR_DELETE_MASTER.getNameRequest())){
                        System.out.println("REQUESR_DELETE_MASTER");
                        ConnectDB.deleteMaster(reqList.get(1), reqList.get(2));
                        history = "Удалили мастера: " + reqList.get(1) + ", с телефоном :" + reqList.get(2);
                    }else if(reqList.get(0).equals(RequestServer.REQUESR_ADD_RECORD.getNameRequest())){
                        System.out.println("REQUESR_ADD_RECORD");
                        ConnectDB.addRecord(reqList.get(1), reqList.get(2), reqList.get(3), reqList.get(4), reqList.get(5),
                                reqList.get(6), reqList.get(7), reqList.get(8));
                        history = "Добавили записи на: " + reqList.get(1) + ", на время: " + reqList.get(2) +
                                ", клиент: " + reqList.get(5) + ", к мастеру: " + reqList.get(4);
                    }else if(reqList.get(0).equals(RequestServer.REQUESR_EDIT_RECORD.getNameRequest())){
                        System.out.println("REQUESR_EDIT_RECORD");
                        ConnectDB.editRecord(reqList.get(1), reqList.get(2), reqList.get(3), reqList.get(4), reqList.get(5));
                        history = "Изменили записи на: " + reqList.get(1) + ", на время: " + reqList.get(2) +
                                ", клиент: " + reqList.get(4) + ", к мастеру: " + reqList.get(3);
                    }else if(reqList.get(0).equals(RequestServer.REQUESR_DELETE_RECORD.getNameRequest())){
                        System.out.println("REQUESR_DELETE_RECORD");
                        ConnectDB.deleteRecord(reqList.get(1), reqList.get(2), reqList.get(3));
                        history = "Удалили записи на: " + reqList.get(1) + ", на время: " + reqList.get(2) +
                                ", у мастера: " + reqList.get(3);
                    }else if(reqList.get(0).equals(RequestServer.REQUESR_GET_ALL_PROCEDURE.getNameRequest())){
                        System.out.println("REQUESR_GET_ALL_PROCEDURE");
                        resultStrings = ConnectDB.getAllProcedure();
                    }else if(reqList.get(0).equals(RequestServer.REQUESR_GET_REPORT.getNameRequest())){
                        System.out.println("REQUESR_GET_REPORT");
                        resultStrings = ConnectDB.getReport(reqList.get(1), reqList.get(2), reqList.get(3), reqList.get(4));
                    }else if(reqList.get(0).equals(RequestServer.REQUESR_GET_HISTORY.getNameRequest())){
                        System.out.println("REQUESR_GET_HISTORY");
                        resultStrings = ConnectDB.getHistory();
                    }else if(reqList.get(0).equals(RequestServer.REQUESR_MOVE_RECORD.getNameRequest())){
                        System.out.println("REQUESR_MOVE_RECORD");
                        ConnectDB.moveRecord(reqList.get(1), reqList.get(2), reqList.get(3), reqList.get(4), reqList.get(5), reqList.get(6), reqList.get(7));
                        history = "Перенесли записи с: " + reqList.get(1) + ", с времени: " + reqList.get(2) +
                                " на " + reqList.get(5) + ", на время " + reqList.get(6) + ", у мастера " + reqList.get(4);
                    }
                    if(!history.equals("")){
                        ConnectDB.addHistory(history);
                    }

                    if (entry.equalsIgnoreCase("quit")) {
                        out.writeUTF(entry);
                        out.flush();
                        break;
                    } else {
                        String response = "";
                        for(String str: resultStrings){
                            response +=  str + "|";
                        }
                        out.writeUTF(response);
                    }
                }
                out.flush();
                System.out.println("READ from clientDialog message - " + entry);
            }

            System.out.println("Client disconnected");
            System.out.println("Closing connections & channels.");

            in.close();
            out.close();

            close();

            System.out.println("Closing connections & channels - DONE.");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
