/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 *
 * BlockConsistencyCheck is design for consistency check.
 */
public class BlockConsistencyCheck extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(BlockConsistencyCheck.class);
  long mBlockId;
  private InStreamOptions mOptions;
  private Map<WorkerNetAddress, Long> mFailedWorkers = new HashMap<>();
  private AlluxioBlockStore mBlockStore;
  private BlockInStream mBlockInStream;
  private BlockChecksumCompute mBlockChecksumCompute;

  /**
   *
   * @param blockStore blockStore
   * @param mFiled filed worker
   * @param mOption options
   * @param blockId block ID
   */
  public BlockConsistencyCheck(AlluxioBlockStore blockStore, Map<WorkerNetAddress, Long> mFiled,
      InStreamOptions mOption, long blockId) {
    mBlockId = blockId;
    mOptions = mOption;
    mFailedWorkers = mFiled;
    mBlockStore = blockStore;
  }

  @Override
  public void run() {
    try {
      init();
      ExecutorService execPoll = Executors.newCachedThreadPool();
      Future<String> result = execPoll.submit(mBlockChecksumCompute);
      String digest = result.get();
      boolean isConsitenct = mBlockStore.blockConsistencyCheck(mBlockId, digest);
      if (isConsitenct) {
        LOG.info("block {} is consistency. ", mBlockId);
      } else {
        LOG.info("block {} is inconsistency.", mBlockId);
        Preconditions.checkState(false, "block %s is inconsistent", mBlockId);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void init() throws IOException {
    mBlockInStream = mBlockStore.getInStream(mBlockId, mOptions, mFailedWorkers);
    mBlockChecksumCompute = new BlockChecksumCompute(mBlockInStream);
  }
}
