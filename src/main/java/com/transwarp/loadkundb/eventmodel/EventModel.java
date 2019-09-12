package com.transwarp.loadkundb.eventmodel;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public abstract class EventModel {
    /**the starting line number as a condition on enabling consumer function, 0-based*/
    protected int startRow;
    /**Each time a row of data is read, the function interface is called to execute the custom logic.*/
    protected Consumer<List<byte[]>> readConsumer;
    /**Store record values that appear in a row of cells */
    protected List<byte[]> rowCellValues;

    public EventModel(int startRow, Consumer<List<byte[]>> readConsumer) {
        this.startRow = startRow;
        this.readConsumer = readConsumer;
        this.rowCellValues = new ArrayList<byte[]>();
    }

    /**
     * Initiates the processing of the Excel file to Bean.
     * @throws Exception
     */
    public abstract void process() throws Exception;

    /**
     * 启用消费函数的起始行
     * @param startRow
     * @return
     */
    public EventModel startRow(int startRow){
        this.startRow = startRow;
        return this;
    }

    /**
     * 设置消费函数。每读完一行记录（one row of records），调用该函数。
     * @param readConsumer
     * @return
     */
    public EventModel readConsumer(Consumer<List<byte[]>> readConsumer){
        this.readConsumer = readConsumer;
        return this;
    }
}
