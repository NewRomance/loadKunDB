package com.transwarp.loadkundb.eventmodel;

import com.transwarp.loadkundb.common.ShardedTableInfo;
import org.apache.poi.hssf.eventusermodel.*;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.model.HSSFFormulaParser;
import org.apache.poi.hssf.record.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class XLSEventModel extends EventModel implements HSSFListener, ShardedTableInfo {
    private static Logger log = LoggerFactory.getLogger(com.github.liuhuagui.gridexcel.eventmodel.XLSEventModel.class);
    private POIFSFileSystem fs;
    /** Should we output the formula, or the value it has? */
    private boolean outputFormulaValues = true;

    /** For parsing Formulas */
    private EventWorkbookBuilder.SheetRecordCollectingListener workbookBuildingListener;
    private HSSFWorkbook stubWorkbook;

    // Records we pick up as we process
    private SSTRecord sstRecord;
    private FormatTrackingHSSFListener formatListener;

    /** So we known which sheet we're on */
    private int sheetIndex = -1;
    private BoundSheetRecord[] orderedBSRs;
    private List<BoundSheetRecord> boundSheetRecords = new ArrayList<>();

    // For handling formulas with string results
    private boolean outputNextStringRecord;

    /**
     * Creates a new XLS -> Bean converter
     * @param fs The POIFSFileSystem to process
     * @param startRow the starting line number as a condition on enabling consumer function, 0-based
     * @param readConsumer Each time a row of data is read, the function interface is called to execute the custom logic.
     */
    public XLSEventModel(POIFSFileSystem fs, int startRow, Consumer<List<byte[]>> readConsumer) {
        super(startRow,readConsumer);
        this.fs = fs;
    }

    /**
     * Creates a new XLS -> Bean converter
     * @param stream the input stream of .xlsx file.
     * @param startRow the starting line number as a condition on enabling consumer function, 0-based
     * @param readConsumer Each time a row of data is read, the function interface is called to execute the custom logic.
     * @throws IOException
     */
    public XLSEventModel(InputStream stream, int startRow, Consumer<List<byte[]>> readConsumer) throws IOException{
        this(new POIFSFileSystem(stream),startRow,readConsumer);
    }

    /**
     * Creates a new XLS -> Bean converter，default <code>startRow=0</code>
     * @param stream the input stream of .xlsx file.
     * @param readConsumer Each time a row of data is read, the function interface is called to execute the custom logic.
     * @throws IOException
     */
    public XLSEventModel(InputStream stream,Consumer<List<byte[]>> readConsumer) throws IOException{
        this(stream,0,readConsumer);
    }

    /**
     * Creates a new XLS -> Bean converter，default <code>startRow=0,readConsumer=null</code>
     * @param stream the input stream of .xlsx file.
     * @throws IOException
     */
    public XLSEventModel(InputStream stream) throws IOException{
        this(stream,null);
    }

    @Override
    public void process() throws IOException {
        MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this);
        formatListener = new FormatTrackingHSSFListener(listener);

        HSSFEventFactory factory = new HSSFEventFactory();
        HSSFRequest request = new HSSFRequest();

        if(outputFormulaValues) {
            request.addListenerForAllRecords(formatListener);
        } else {
            workbookBuildingListener = new EventWorkbookBuilder.SheetRecordCollectingListener(formatListener);
            request.addListenerForAllRecords(workbookBuildingListener);
        }

        factory.processWorkbookEvents(request, fs);
    }

    /**
     * Main HSSFListener method, processes events, and outputs the
     *  Bean as the file is processed.
     */
    @Override
    public void processRecord(Record record) {
        byte[] thisStr = null;

        switch (record.getSid())
        {
            case BoundSheetRecord.sid:
                boundSheetRecords.add((BoundSheetRecord)record);
                break;
            case BOFRecord.sid:
                BOFRecord br = (BOFRecord)record;
                if(br.getType() == BOFRecord.TYPE_WORKSHEET) {
                    // Create sub workbook if required
                    if(workbookBuildingListener != null && stubWorkbook == null) {
                        stubWorkbook = workbookBuildingListener.getStubHSSFWorkbook();
                    }

                    // Output the worksheet name
                    // Works by ordering the BSRs by the location of
                    //  their BOFRecords, and then knowing that we
                    //  process BOFRecords in byte offset order
                    sheetIndex++;
                    if(orderedBSRs == null) {
                        orderedBSRs = BoundSheetRecord.orderByBofPosition(boundSheetRecords);
                    }

                    // get the sheet name
                   // tableNamelist.add(orderedBSRs[sheetIndex].getSheetname());
                    thisStr = orderedBSRs[sheetIndex].getSheetname().getBytes();
                    log.info(orderedBSRs[sheetIndex].getSheetname());
                }
                break;

            case SSTRecord.sid:
                sstRecord = (SSTRecord) record;
                break;

            case BlankRecord.sid:
//			BlankRecord brec = (BlankRecord) record;
                thisStr = "".getBytes();
                break;
            case BoolErrRecord.sid:
//			BoolErrRecord berec = (BoolErrRecord) record;
                thisStr = "".getBytes();
                break;

            case FormulaRecord.sid:
                FormulaRecord frec = (FormulaRecord) record;
                if(outputFormulaValues) {
                    if(Double.isNaN( frec.getValue() )) {
                        // Formula result is a string
                        // This is stored in the next record
                        outputNextStringRecord = true;
                    } else {
                        thisStr = formatListener.formatNumberDateCell(frec).getBytes();
                    }
                } else {
                    thisStr = HSSFFormulaParser.toFormulaString(stubWorkbook, frec.getParsedExpression()).getBytes();
                }
                break;
            case StringRecord.sid:
                if(outputNextStringRecord) {
                    // String for formula
                    StringRecord srec = (StringRecord)record;
                    thisStr = srec.getString().getBytes();
                    outputNextStringRecord = false;
                }
                break;

            case LabelRecord.sid:  //get cell value
                LabelRecord lrec = (LabelRecord) record;
                thisStr = lrec.getValue().getBytes();
                break;
            case LabelSSTRecord.sid:
                LabelSSTRecord lsrec = (LabelSSTRecord) record;
                if(sstRecord == null) {
                    thisStr = "(No SST Record, can't identify string)".getBytes();
                } else {
                    thisStr = sstRecord.getString(lsrec.getSSTIndex()).toString().getBytes();
                }
                break;
            case NoteRecord.sid:
//			NoteRecord nrec = (NoteRecord) record;
                // TODO: Find object to match nrec.getShapeId()
                thisStr = "(TODO)".getBytes();
                break;
            case NumberRecord.sid:
                NumberRecord numrec = (NumberRecord) record;
                // Format
                thisStr = formatListener.formatNumberDateCell(numrec).getBytes();
                break;
            case RKRecord.sid:
//			RKRecord rkrec = (RKRecord) record;
                thisStr = "(TODO)".getBytes();
                break;
            default:
                break;
        }

        // Handle missing column
        if(record instanceof MissingCellDummyRecord) {
//			MissingCellDummyRecord mc = (MissingCellDummyRecord)record;
            thisStr = null;
        }

        // If we got something to store it into rowCellValues.
        if(thisStr != null) {
            rowCellValues.add(thisStr);
        }

        // Handle end of row
        if(record instanceof LastCellOfRowDummyRecord) {
            // End the row
            //If the current line number≥ startRow, then the consumer function is enabled,
            //otherwise the entire line record is directly discarded and is not processed.
            if(((LastCellOfRowDummyRecord) record).getRow()>=startRow){
                readConsumer.accept(rowCellValues);
            }
            // init new row
            rowCellValues = new ArrayList<byte[]>();
        }
    }

}
