package com.kinesis.dat.kinesisdat.service;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.kinesis.dat.kinesisdat.model.RecordModal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

@Service
public class DatReaderService {

    static final Logger logger = LoggerFactory.getLogger(DatReaderService.class);

    @Autowired
    private ObjectMapper mapper;
    public void writeToKinesis(){
        String streamName = "kinesis-dat-reader-stream";
        String region = "us-east-1";
        logger.info(String.format("Starting Kinesis Producer Library Application for Stream %s in %s", streamName, region));

        var producerConfiguration=new KinesisProducerConfiguration().setRegion(Region.US_EAST_1.toString());
        producerConfiguration.setAggregationEnabled(false);
        var producer =new KinesisProducer(producerConfiguration);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Shutting down program ");
            producer.flush();
        },"producer-shutdown"));

        List<Future<UserRecordResult>> putFeatures =new LinkedList<>();
        int size  = getRecordList().size();
        int i = 0;
        while(i < size){
            var recordModal = getRecordList().get(i);
            logger.info(String.format("Generated %s", recordModal));

            ByteBuffer data = null;
            try{
                data =ByteBuffer.wrap(mapper.writeValueAsBytes(recordModal));
            }catch (Exception e){
                logger.error(String.format("Failed to serialize %s", recordModal), e);
                continue;
            }
            ListenableFuture<UserRecordResult> future =
                    producer.addUserRecord(streamName,recordModal.getRecordId(),data);
            putFeatures.add(future);
            Futures.addCallback(future, new FutureCallback<UserRecordResult>() {
                @Override
                public void onSuccess(@Nullable UserRecordResult userRecordResult) {
                    logger.info(String.format("Produced User Record to shard %s at position %s",
                            userRecordResult.getShardId(),
                            userRecordResult.getSequenceNumber()));
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error("Failed to produce batch", throwable);
                }
            }, MoreExecutors.directExecutor());
            if (++i % 100 == 0) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.warn("Sleep interrupted", e);
                }
            }
        }
    }

    public List<RecordModal> getRecordList(){
        List<RecordModal> recordModalList = new ArrayList<>();
        RecordModal recordModal = new RecordModal();
        recordModal.setRecordId(UUID.randomUUID().toString());
        recordModal.setRecordData(Arrays.asList("flu","alga","chin","kruel"));
        recordModal.setRecordName("kinesis-record");
        recordModalList.add(recordModal);

        RecordModal recordModal1 = new RecordModal();
        recordModal1.setRecordId(UUID.randomUUID().toString());
        recordModal1.setRecordData(Arrays.asList("2 flu","2 alga","2 chin","2 kruel"));
        recordModal1.setRecordName("kinesis-record-2");
        recordModalList.add(recordModal1);

        RecordModal recordModal3 = new RecordModal();
        recordModal1.setRecordId(UUID.randomUUID().toString());
        recordModal1.setRecordData(Arrays.asList("3 flu","3 alga","3 chin","3 kruel"));
        recordModal1.setRecordName("kinesis-record-3");
        recordModalList.add(recordModal3);

        return recordModalList;
    }
}
