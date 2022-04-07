package com.hc.cdc.data;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.BoxedWrapperRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;

public class StreamExecCalc$21 extends TableStreamOperator
        implements OneInputStreamOperator {

    private final Object[] references;
    private transient StringDataSerializer typeSerializer$12;
    private transient StringDataSerializer typeSerializer$19;
    BoxedWrapperRowData out = new BoxedWrapperRowData(5);
    private final StreamRecord outElement = new StreamRecord(null);

    public StreamExecCalc$21(
            Object[] references,
            StreamTask task,
            StreamConfig config,
            Output output,
            ProcessingTimeService processingTimeService) throws Exception {
        this.references = references;
        typeSerializer$12 = (((StringDataSerializer) references[0]));
        typeSerializer$19 = (((StringDataSerializer) references[1]));
        this.setup(task, config, output);
        if (this instanceof AbstractStreamOperator) {
            ((AbstractStreamOperator) this)
                    .setProcessingTimeService(processingTimeService);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();

    }

    @Override
    public void processElement(StreamRecord element) throws Exception {
        RowData in1 = (RowData) element.getValue();

        BinaryStringData field$11;
        boolean isNull$11;
        BinaryStringData field$13;
        BinaryStringData field$14;
        boolean isNull$14;
        BinaryStringData field$15;
        TimestampData field$16;
        boolean isNull$16;
        long field$17;
        boolean isNull$17;
        BinaryStringData field$18;
        boolean isNull$18;
        BinaryStringData field$20;


        isNull$17 = in1.isNullAt(0);
        field$17 = -1L;
        if (!isNull$17) {
            field$17 = in1.getLong(0);
        }

        isNull$11 = in1.isNullAt(2);
        field$11 = BinaryStringData.EMPTY_UTF8;
        if (!isNull$11) {
            field$11 = ((BinaryStringData) in1.getString(2));
        }
        field$13 = field$11;
        if (!isNull$11) {
            field$13 = (BinaryStringData) (typeSerializer$12.copy(field$13));
        }


        isNull$14 = in1.isNullAt(4);
        field$14 = BinaryStringData.EMPTY_UTF8;
        if (!isNull$14) {
            field$14 = ((BinaryStringData) in1.getString(4));
        }
        field$15 = field$14;
        if (!isNull$14) {
            field$15 = (BinaryStringData) (typeSerializer$12.copy(field$15));
        }


        isNull$18 = in1.isNullAt(1);
        field$18 = BinaryStringData.EMPTY_UTF8;
        if (!isNull$18) {
            field$18 = ((BinaryStringData) in1.getString(1));
        }
        field$20 = field$18;
        if (!isNull$18) {
            field$20 = (BinaryStringData) (typeSerializer$19.copy(field$20));
        }

        isNull$16 = in1.isNullAt(3);
        field$16 = null;
        if (!isNull$16) {
            field$16 = in1.getTimestamp(3, 3);
        }

        out.setRowKind(in1.getRowKind());




        if (isNull$11) {
            out.setNullAt(0);
        } else {
            out.setNonPrimitiveValue(0, field$13);
        }



        if (isNull$14) {
            out.setNullAt(1);
        } else {
            out.setNonPrimitiveValue(1, field$15);
        }



        if (isNull$16) {
            out.setNullAt(2);
        } else {
            out.setNonPrimitiveValue(2, field$16);
        }



        if (isNull$17) {
            out.setNullAt(3);
        } else {
            out.setLong(3, field$17);
        }



        if (isNull$18) {
            out.setNullAt(4);
        } else {
            out.setNonPrimitiveValue(4, field$20);
        }


        output.collect(outElement.replace(out));


    }



    @Override
    public void close() throws Exception {
        super.close();

    }


}
