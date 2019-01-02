package alluxio.client.block;

import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.wire.WorkerNetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class BlockChecksumStore extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(BlockChecksumCompute.class);
    long blockId;
    private InStreamOptions mOptions;
    private Map<WorkerNetAddress, Long> mFailedWorkers = new HashMap<>();
    private AlluxioBlockStore mBlockStore;
    private BlockInStream mBlockInStream;
    private BlockChecksumCompute blockChecksumCompute;

    public BlockChecksumStore(AlluxioBlockStore blockStore, Map<WorkerNetAddress, Long> mFiled,
                              InStreamOptions mOptions, long blockId) {
        this.blockId = blockId;
        this.mOptions = mOptions;
        this.mFailedWorkers = mFiled;
        this.mBlockStore = blockStore;
    }

    @Override
    public void run() {
        try {
            init();
            ExecutorService execPoll = Executors.newCachedThreadPool();
            Future<String> result = execPoll.submit(blockChecksumCompute);
            String digest = result.get();
            mBlockStore.blockChecksumStore(blockId, digest);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init() throws IOException {
        this.mBlockInStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
        this.blockChecksumCompute = new BlockChecksumCompute(mBlockInStream);
    }
}
