package alluxio.client.block;

public class BlockChecksumCompute extends Thread {

    private AlluxioBlockStore mBlockStore;

    BlockChecksumCompute(AlluxioBlockStore alluxioBlockStore) {
        this.mBlockStore = alluxioBlockStore;
    }

    @Override
    public void run() {

    }

    private String computeDigest() {

        return "";
    }
}
