package com.databricks.labs.guidewire

import org.apache.parquet.io.{DelegatingSeekableInputStream, InputFile, SeekableInputStream}

import java.io.ByteArrayInputStream

class ParquetStream(data: Array[Byte]) extends InputFile {

  override def getLength: Long = data.length

  override def newStream(): SeekableInputStream = {
    new DelegatingSeekableInputStream(new SeekableByteArrayInputStream(this.data)) {
      override def seek(newPos: Long): Unit =
        this.getStream.asInstanceOf[SeekableByteArrayInputStream].setPos(newPos.toInt)

      override def getPos: Long =
        this.getStream.asInstanceOf[SeekableByteArrayInputStream].getPos
    }
  }

  private class SeekableByteArrayInputStream(buf: Array[Byte]) extends ByteArrayInputStream(buf) {
    def getPos: Int = this.pos

    def setPos(pos: Int): Unit = this.pos = pos
  }
}
