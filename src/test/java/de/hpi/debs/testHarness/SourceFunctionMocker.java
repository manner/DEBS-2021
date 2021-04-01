package de.hpi.debs.testHarness;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ConcurrentLinkedQueue;

public class SourceFunctionMocker<T> implements SourceFunction.SourceContext<T>{
    private TypeSerializer<T> outputSerializer;
    protected ConcurrentLinkedQueue<Object> outputList;
    protected final ExecutionConfig executionConfig;

    public SourceFunctionMocker() {
        MockEnvironment env = new MockEnvironmentBuilder()
                .setTaskName("MockTask")
                .setManagedMemorySize(3 * 1024 * 1024)
                .setInputSplitProvider(new MockInputSplitProvider())
                .setBufferSize(1024)
                .setMaxParallelism(1)
                .setParallelism(1)
                .setSubtaskIndex(0)
                .build();

        this.outputList = new ConcurrentLinkedQueue<>();

        this.executionConfig = env.getExecutionConfig();
    }

    @Override
    public void collect(T element) {
        if (outputSerializer == null) {
            outputSerializer =
                    TypeExtractor.getForObject(element)
                            .createSerializer(executionConfig);
        }

        outputList.add(new StreamRecord<>(outputSerializer.copy(element)));
    }

    @Override
    public void collectWithTimestamp(T element, long timestamp) {
        if (outputSerializer == null) {
            outputSerializer =
                    TypeExtractor.getForObject(element)
                            .createSerializer(executionConfig);
        }

        outputList.add(
                new StreamRecord<>(
                        outputSerializer.copy(element), timestamp));
    }

    @Override
    public void emitWatermark(Watermark mark) {
        outputList.add(mark);
    }

    @Override
    public void markAsTemporarilyIdle() {

    }

    @Override
    public Object getCheckpointLock() {
        return null;
    }

    @Override
    public void close() {

    }

    public ConcurrentLinkedQueue<Object> getOutput() {
        return outputList;
    }
}
