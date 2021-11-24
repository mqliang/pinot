/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.spi.memory;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Random;
import javax.annotation.concurrent.ThreadSafe;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


@ThreadSafe
public class PinotNativeOrderLBuffer extends BasePinotLBuffer {

  private static final String RESOURCES_PATH =
      Paths.get(".").toAbsolutePath().toString() + "/pinot-perf/src/main/resources/";
  private static final File TEST_FILE = new File(RESOURCES_PATH + "PinotDataBufferTest.txt");
  private static final long FILE_SIZE_GB = 125;
  private static final int VALUE = Integer.MAX_VALUE;
  private static final Random RANDOM = new Random();
  private static final long FILE_SIZE_BYTES = FILE_SIZE_GB * 1024 * 1024 * 1024;
  private static final long PAGE_SIZE_BYTES = 4 * 1024;
  private static final long PAGE_NUM = FILE_SIZE_BYTES / PAGE_SIZE_BYTES;

  public static void main(String[] args) throws IOException {
    init();

    long sequentialDenseReadStart = System.currentTimeMillis();
    pinotDataBufferMMapSequentialDenseRead();
    long sequentialDenseReadEnd = System.currentTimeMillis();
    System.out.printf("Dense sequential read %d GB data: %s ms\n", FILE_SIZE_GB,
        sequentialDenseReadEnd - sequentialDenseReadStart);

    /*
    long randomDenseReadStart = System.currentTimeMillis();
    pinotDataBufferMMapRandomDenseRead();
    long randomDenseReadEnd = System.currentTimeMillis();
    System.out.printf("Dense random read %d GB data: %s ms\n", FILE_SIZE_GB, randomDenseReadEnd - randomDenseReadStart);
     */

    long sequentialSparseReadStart = System.currentTimeMillis();
    pinotDataBufferMMapSequentialSparseRead();
    long sequentialSparseReadEnd = System.currentTimeMillis();
    System.out.printf("Sparse sequential read %d GB data: %s ms\n", FILE_SIZE_GB,
        sequentialSparseReadEnd - sequentialSparseReadStart);

    long randomSparseReadStart = System.currentTimeMillis();
    pinotDataBufferMMapRandomSparseRead();
    long randomSparseReadEnd = System.currentTimeMillis();
    System.out
        .printf("Sparse random read %d GB data: %s ms\n", FILE_SIZE_GB, randomSparseReadEnd - randomSparseReadStart);
  }

  // Sequentially read all the integers from the file
  public static void pinotDataBufferMMapSequentialDenseRead()
      throws IOException {
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
      long totalNums = FILE_SIZE_BYTES / Integer.BYTES;
      for (long i = 0; i < totalNums; i++) {
        long offset = i * Integer.BYTES;
        int actualValue = buffer.getInt(offset);
        // Just in case of JVM optimizing `buffer.getInt(offset);`
        Preconditions.checkState(actualValue == VALUE);
      }
    }
  }

  // Randomly read all the integers from the file
  public static void pinotDataBufferMMapRandomDenseRead()
      throws IOException {
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
      long totalNums = FILE_SIZE_BYTES / Integer.BYTES;
      for (long i = 0; i < totalNums; i++) {
        long offset = (long) (RANDOM.nextFloat() * totalNums) * Integer.BYTES;
        int actualValue = buffer.getInt(offset);
        // Just in case of JVM optimizing `buffer.getInt(offset);`
        Preconditions.checkState(actualValue == VALUE);
      }
    }
  }

  // Sequentially load all the 4k size pages from disk, for each page just read the first integer.
  public static void pinotDataBufferMMapSequentialSparseRead()
      throws IOException {
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
      for (long i = 0; i < PAGE_NUM; i++) {
        long offset = i * PAGE_SIZE_BYTES;
        int actualValue = buffer.getInt(offset);
        // Just in case of JVM optimizing `buffer.getInt(offset);`
        Preconditions.checkState(actualValue == VALUE);
      }
    }
  }

  // Randomly load all the 4k size pages from disk, for each page just read the first integer.
  public static void pinotDataBufferMMapRandomSparseRead()
      throws IOException {
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
      for (int i = 0; i < PAGE_NUM; i++) {
        long offset = (long) (RANDOM.nextFloat() * PAGE_NUM) * PAGE_SIZE_BYTES;
        int actualValue = buffer.getInt(offset);
        // Just in case of JVM optimizing `buffer.getInt(offset);`
        Preconditions.checkState(actualValue == VALUE);
      }
    }
  }

  // Create a file with given size on disk, and fill the file with Integer.MAX_VALUE
  // NOTE: not use "randomAccessFile.setLength()" --> JVM/OS will create a sparse file
  public static void init() {
    if (!TEST_FILE.exists() || TEST_FILE.length() != FILE_SIZE_BYTES) {
      System.out.println("Initialization start.");
      try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEST_FILE, false, 0, FILE_SIZE_BYTES)) {
        long offset = 0;
        while (offset != FILE_SIZE_BYTES) {
          buffer.putInt(offset, VALUE);
          offset += Integer.BYTES;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    Preconditions.checkState(FILE_SIZE_BYTES == TEST_FILE.length());
    System.out.println("Initialization completed.");
  }

  static PinotNativeOrderLBuffer allocateDirect(long size) {
    LBufferAPI buffer = new LBuffer(size);
    return new PinotNativeOrderLBuffer(buffer, true, false);
  }

  static PinotNativeOrderLBuffer loadFile(File file, long offset, long size)
      throws IOException {
    PinotNativeOrderLBuffer buffer = allocateDirect(size);
    buffer.readFrom(0, file, offset, size);
    return buffer;
  }

  public static PinotNativeOrderLBuffer mapFile(File file, boolean readOnly, long offset, long size)
      throws IOException {
    if (readOnly) {
      return new PinotNativeOrderLBuffer(new MMapBuffer(file, offset, size, MMapMode.READ_ONLY), true, false);
    } else {
      return new PinotNativeOrderLBuffer(new MMapBuffer(file, offset, size, MMapMode.READ_WRITE), true, true);
    }
  }

  PinotNativeOrderLBuffer(LBufferAPI buffer, boolean closeable, boolean flushable) {
    super(buffer, closeable, flushable);
  }

  @Override
  public char getChar(int offset) {
    return _buffer.getChar(offset);
  }

  @Override
  public char getChar(long offset) {
    return _buffer.getChar(offset);
  }

  @Override
  public void putChar(int offset, char value) {
    _buffer.putChar(offset, value);
  }

  @Override
  public void putChar(long offset, char value) {
    _buffer.putChar(offset, value);
  }

  @Override
  public short getShort(int offset) {
    return _buffer.getShort(offset);
  }

  @Override
  public short getShort(long offset) {
    return _buffer.getShort(offset);
  }

  @Override
  public void putShort(int offset, short value) {
    _buffer.putShort(offset, value);
  }

  @Override
  public void putShort(long offset, short value) {
    _buffer.putShort(offset, value);
  }

  @Override
  public int getInt(int offset) {
    return _buffer.getInt(offset);
  }

  @Override
  public int getInt(long offset) {
    return _buffer.getInt(offset);
  }

  @Override
  public void putInt(int offset, int value) {
    _buffer.putInt(offset, value);
  }

  @Override
  public void putInt(long offset, int value) {
    _buffer.putInt(offset, value);
  }

  @Override
  public long getLong(int offset) {
    return _buffer.getLong(offset);
  }

  @Override
  public long getLong(long offset) {
    return _buffer.getLong(offset);
  }

  @Override
  public void putLong(int offset, long value) {
    _buffer.putLong(offset, value);
  }

  @Override
  public void putLong(long offset, long value) {
    _buffer.putLong(offset, value);
  }

  @Override
  public float getFloat(int offset) {
    return _buffer.getFloat(offset);
  }

  @Override
  public float getFloat(long offset) {
    return _buffer.getFloat(offset);
  }

  @Override
  public void putFloat(int offset, float value) {
    _buffer.putFloat(offset, value);
  }

  @Override
  public void putFloat(long offset, float value) {
    _buffer.putFloat(offset, value);
  }

  @Override
  public double getDouble(int offset) {
    return _buffer.getDouble(offset);
  }

  @Override
  public double getDouble(long offset) {
    return _buffer.getDouble(offset);
  }

  @Override
  public void putDouble(int offset, double value) {
    _buffer.putDouble(offset, value);
  }

  @Override
  public void putDouble(long offset, double value) {
    _buffer.putDouble(offset, value);
  }

  @Override
  public ByteOrder order() {
    return NATIVE_ORDER;
  }
}
