package com.databricks.labs.guidewire;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.ByteArrayInputStream;

public class ParquetStream implements InputFile {

    private final byte[] data;

    public ParquetStream(byte[] data) {
        this.data = data;
    }

    @Override
    public long getLength() {
        return this.data.length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new DelegatingSeekableInputStream(new SeekableByteArrayInputStream(this.data)) {
            @Override
            public void seek(long newPos) {
                ((SeekableByteArrayInputStream) this.getStream()).setPos((int) newPos);
            }

            @Override
            public long getPos() {
                return ((SeekableByteArrayInputStream) this.getStream()).getPos();
            }
        };
    }

    private static class SeekableByteArrayInputStream extends ByteArrayInputStream {
        public SeekableByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        public int getPos() {
            return this.pos;
        }

        public void setPos(int pos) {
            this.pos = pos;
        }
    }
}