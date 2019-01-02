package alluxio.client.block;

import alluxio.client.block.stream.BlockInStream;

import java.security.MessageDigest;
import java.util.concurrent.Callable;

public class BlockChecksumCompute implements Callable<String> {
    private BlockInStream mBlockInSteam;

    public BlockChecksumCompute(BlockInStream mSteam) {
        this.mBlockInSteam = mSteam;
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
