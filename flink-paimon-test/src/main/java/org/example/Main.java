package org.example;

import com.fan.lakehouse.dw.ods2dwdDemo;
import com.fan.lakehouse.read.ReadFromTable;
import com.fan.lakehouse.write.WriteToTable;

public class Main {
    public static void main(String[] args) throws Exception {
        ods2dwdDemo.writeToDwd();
    }
}