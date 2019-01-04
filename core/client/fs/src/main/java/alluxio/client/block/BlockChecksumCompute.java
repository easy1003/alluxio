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

import java.security.MessageDigest;
import java.util.concurrent.Callable;

/**
 *  BlockChecksumCompute is design for checksum compute.
 */
public class BlockChecksumCompute implements Callable<String> {
  private BlockInStream mBlockInSteam;

  /**
   *
   * @param mSteam blockInStream
   */
  public BlockChecksumCompute(BlockInStream mSteam) {
    mBlockInSteam = mSteam;
  }

  @Override
  public String call() throws Exception {
    byte[] buff = new byte[512];
    int read = 0;
    MessageDigest msg = MessageDigest.getInstance("MD5");
    while ((read = mBlockInSteam.read(buff)) != -1) {
      msg.update(buff, 0, read);
    }
    byte[] hashDigest = msg.digest();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < hashDigest.length; i++) {
      int v = hashDigest[i] & 0xFF;
      if (v < 16) {
        sb.append("0");
      }
      sb.append(Integer.toString(v, 16).toUpperCase());
    }
    return sb.toString();
  }
}
