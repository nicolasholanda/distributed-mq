package com.github.nicolasholanda.mq.broker.log;

import com.github.nicolasholanda.mq.common.record.RecordBatch;
import java.util.List;

public record FetchDataInfo(long fetchOffset, long highWatermark, long logStartOffset, List<RecordBatch> batches) {

    public FetchDataInfo {
        batches = List.copyOf(batches);
    }

    public boolean isEmpty() {
        return batches.isEmpty();
    }

    public int sizeInBytes() {
        int size = 0;
        for (RecordBatch batch : batches) {
            size += batch.sizeInBytes();
        }
        return size;
    }
}
