package cn.weeg.exp.databaseDesign.impl2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author weeGiam
 */

public class BPlusTreeImpl implements BPlusTree {
    private DiskManager diskManager;
    private BufferPoolManager bufferPool;
    private int rootPageId;
    private int nextPageId;
    private int pageSize;
    private String currentFilename;
    private int maxKeysPerNode;

    // 统计信息
    private int splitCount;
    private int mergeCount;

    // 递归深度限制
    private static final int MAX_RECURSION_DEPTH = 50;

    public BPlusTreeImpl() {
        this.diskManager = new DiskManager();
        this.rootPageId = -1;
        this.nextPageId = 0;
        this.splitCount = 0;
        this.mergeCount = 0;
    }

    @Override
    public void create(String filename, int pageSize) {
        try {
            this.pageSize = pageSize;
            this.currentFilename = filename;
            this.maxKeysPerNode = calculateMaxKeys(pageSize);

            diskManager.openFile(filename, pageSize);
            bufferPool = new BufferPoolManager(100, pageSize, diskManager);

            // 读取或创建元数据
            byte[] metadata = diskManager.readMetadata();
            ByteBuffer metaBuffer = ByteBuffer.wrap(metadata);

            int magic = metaBuffer.getInt(0);
            if (magic == 0x12345678) {
                // 文件已存在，读取元数据
                rootPageId = metaBuffer.getInt(4);
                nextPageId = metaBuffer.getInt(8);
                splitCount = metaBuffer.getInt(12);
                mergeCount = metaBuffer.getInt(16);

                // 验证树结构的完整性
                if (rootPageId >= 0) {
                    validateTreeStructure();
                }
            } else {
                // 新文件，初始化元数据
                rootPageId = -1;
                nextPageId = 0;
                splitCount = 0;
                mergeCount = 0;
                saveMetadata();
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to create/open B+ tree file: " + filename, e);
        }
    }

    @Override
    public void close(String filename) {
        if (bufferPool != null) {
            bufferPool.flushAllPages();
            saveMetadata();
        }
        try {
            diskManager.closeFile();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close file: " + filename, e);
        }
    }

    @Override
    public void insert(String key, String[] value, String rowId) {
        if (rootPageId == -1) {
            // 创建根节点 - 初始时应该是叶子节点
            rootPageId = allocateNewPage();
            LeafNode root = new LeafNode(rootPageId, maxKeysPerNode);
            root.insertKeyValue(key, value, rowId);

            Page page = bufferPool.fetchPage(rootPageId);
            root.serialize(page);
            bufferPool.unpinPage(rootPageId, true);
            saveMetadata();
            return;
        }

        // 使用访问过的页面集合来检测循环
        Set<Integer> visitedPages = new HashSet<>();
        insertHelper(rootPageId, key, value, rowId, 0, visitedPages);
    }

    private void insertHelper(int nodePageId, String key, String[] value, String rowId,
                              int depth, Set<Integer> visitedPages) {
        if (depth > MAX_RECURSION_DEPTH) {
            throw new RuntimeException("最大递归深度超出：B+树结构可能存在循环，深度: " + depth);
        }

        if (nodePageId < 0) {
            throw new IllegalStateException("无效的节点页面ID: " + nodePageId);
        }

        if (visitedPages.contains(nodePageId)) {
            throw new RuntimeException("检测到循环引用：页面 " + nodePageId + " 已被访问过");
        }

        visitedPages.add(nodePageId);

        try {
            Page page = bufferPool.fetchPage(nodePageId);

            // 判断节点类型
            int nodeType = page.readInt(0);

            if (nodeType == 1) { // 叶子节点
                LeafNode leaf = new LeafNode(nodePageId, maxKeysPerNode);
                leaf.deserialize(page);

                // 修正：无论是否满都先尝试插入
                if (!leaf.isFull()) {
                    // 节点未满，直接插入
                    leaf.insertKeyValue(key, value, rowId);
                    leaf.serialize(page);
                    bufferPool.unpinPage(nodePageId, true);
                    System.out.println("直接插入到未满的叶子节点");
                } else {
                    // 节点已满，需要分裂
                    System.out.println("叶子节点已满，开始分裂过程...");

                    // 先分裂节点
                    LeafNode newLeaf = leaf.split();
                    int newLeafPageId = allocateNewPage();
                    newLeaf.setPageId(newLeafPageId);

                    // 更新叶子节点链表
                    newLeaf.setNextLeafPageId(leaf.getNextLeafPageId());
                    leaf.setNextLeafPageId(newLeafPageId);

                    // 决定将新key插入到哪个节点
                    String firstKeyOfNewLeaf = newLeaf.getKeys()[0];
                    if (key.compareTo(firstKeyOfNewLeaf) < 0) {
                        // 插入到原节点
                        leaf.insertKeyValue(key, value, rowId);
                        System.out.println("新key插入到原叶子节点");
                    } else {
                        // 插入到新节点
                        newLeaf.insertKeyValue(key, value, rowId);
                        System.out.println("新key插入到新叶子节点");
                    }

                    // 序列化两个节点
                    leaf.serialize(page);
                    Page newPage = bufferPool.fetchPage(newLeafPageId);
                    newLeaf.serialize(newPage);

                    bufferPool.unpinPage(nodePageId, true);
                    bufferPool.unpinPage(newLeafPageId, true);

                    splitCount++;

                    // 向父节点插入新的key（使用新叶子节点的第一个key）
                    String newKey = newLeaf.getKeys()[0];
                    System.out.println("向父节点插入key: " + newKey);
                    insertToParent(leaf, newKey, newLeaf);
                }
            } else if (nodeType == 0) { // 内部节点
                InternalNode internal = new InternalNode(nodePageId, maxKeysPerNode);
                internal.deserialize(page);
                bufferPool.unpinPage(nodePageId, false);

                // 调试输出
                System.out.println("处理内部节点 " + nodePageId + ", keyCount: " + internal.getKeyCount());

                int childPageId = internal.findChild(key);
                if (childPageId < 0) {
                    throw new IllegalStateException("内部节点 " + nodePageId + " 返回无效的子节点ID: " + childPageId);
                }
                if (childPageId == nodePageId) {
                    // 打印调试信息
                    internal.printNode();
                    throw new IllegalStateException("内部节点 " + nodePageId + " 自引用");
                }

                // 递归插入到子节点
                insertHelper(childPageId, key, value, rowId, depth + 1, visitedPages);
            } else {
                throw new IllegalStateException("未知的节点类型: " + nodeType + " 在页面: " + nodePageId);
            }
        } finally {
            // 从访问集合中移除当前页面，允许其他路径访问
            visitedPages.remove(nodePageId);
        }
    }

    private void insertToParent(BPlusTreeNode leftNode, String key, BPlusTreeNode rightNode) {
        if (leftNode.getParentPageId() == -1) {
            // 创建新的根节点
            int newRootPageId = allocateNewPage();
            InternalNode newRoot = new InternalNode(newRootPageId, maxKeysPerNode);

            // 正确设置子节点指针 - 确保不会自引用
            if (leftNode.getPageId() == newRootPageId || rightNode.getPageId() == newRootPageId) {
                throw new IllegalStateException("新根节点ID与子节点ID冲突");
            }

            newRoot.insertKeyChild(key, leftNode.getPageId(), rightNode.getPageId());

            // 更新父子关系
            leftNode.setParentPageId(newRootPageId);
            rightNode.setParentPageId(newRootPageId);

            // 序列化新根节点
            Page rootPage = bufferPool.fetchPage(newRootPageId);
            newRoot.serialize(rootPage);
            bufferPool.unpinPage(newRootPageId, true);

            // 更新子节点的父节点信息
            updateNodeParent(leftNode);
            updateNodeParent(rightNode);

            rootPageId = newRootPageId;
            saveMetadata();

            System.out.println("创建新根节点: " + newRootPageId + ", 子节点: " + leftNode.getPageId() + ", " + rightNode.getPageId());
        } else {
            // 向现有父节点插入
            Page parentPage = bufferPool.fetchPage(leftNode.getParentPageId());
            InternalNode parent = new InternalNode(leftNode.getParentPageId(), maxKeysPerNode);
            parent.deserialize(parentPage);

            if (!parent.isFull()) {
                // 父节点未满，直接插入
                parent.insertKeyChild(key, leftNode.getPageId(), rightNode.getPageId());
                rightNode.setParentPageId(parent.getPageId());
                parent.serialize(parentPage);
                bufferPool.unpinPage(parent.getPageId(), true);
                updateNodeParent(rightNode);
                System.out.println("向未满的父节点插入key: " + key);
            } else {
                // 父节点也已满，需要分裂
                System.out.println("父节点已满，开始分裂父节点...");

                // 先将新的key-child插入到父节点（临时超过容量）
                parent.insertKeyChildForSplit(key, leftNode.getPageId(), rightNode.getPageId());
                rightNode.setParentPageId(parent.getPageId());

                // 分裂父节点
                InternalNode newParent = parent.split();
                int newParentPageId = allocateNewPage();
                newParent.setPageId(newParentPageId);

                // 获取提升到上层的中间key
                String midKey = parent.getSplitMiddleKey();

                // 更新分裂后的父子关系
                updateChildrenParent(parent);
                updateChildrenParent(newParent);

                // 序列化两个内部节点
                parent.serialize(parentPage);
                Page newParentPage = bufferPool.fetchPage(newParentPageId);
                newParent.serialize(newParentPage);

                bufferPool.unpinPage(parent.getPageId(), true);
                bufferPool.unpinPage(newParentPageId, true);

                splitCount++;
                System.out.println("父节点分裂完成，中间key: " + midKey);

                // 递归向上插入中间key
                insertToParent(parent, midKey, newParent);
            }
        }
    }
    @Override
    public String[][] get(String key) {
        if (rootPageId == -1) {
            return new String[0][];
        }

        bufferPool.resetPageAccessCount();
        Set<Integer> visitedPages = new HashSet<>();
        String[][] result = searchHelper(rootPageId, key, 0, visitedPages);
        return result;
    }

    private String[][] searchHelper(int nodePageId, String key, int depth, Set<Integer> visitedPages) {
        if (depth > MAX_RECURSION_DEPTH) {
            throw new RuntimeException("查询时最大递归深度超出：B+树结构可能存在循环，深度: " + depth);
        }

        if (nodePageId < 0) {
            return new String[0][];
        }

        if (visitedPages.contains(nodePageId)) {
            throw new RuntimeException("查询时检测到循环引用：页面 " + nodePageId + " 已被访问过");
        }

        visitedPages.add(nodePageId);

        try {
            Page page = bufferPool.fetchPage(nodePageId);
            int nodeType = page.readInt(0);

            if (nodeType == 1) { // 叶子节点
                LeafNode leaf = new LeafNode(nodePageId, maxKeysPerNode);
                leaf.deserialize(page);
                bufferPool.unpinPage(nodePageId, false);
                return leaf.search(key);
            } else if (nodeType == 0) { // 内部节点
                InternalNode internal = new InternalNode(nodePageId, maxKeysPerNode);
                internal.deserialize(page);
                bufferPool.unpinPage(nodePageId, false);

                int childPageId = internal.findChild(key);
                if (childPageId < 0) {
                    throw new IllegalStateException("内部节点 " + nodePageId + " 返回无效的子节点ID: " + childPageId);
                }
                if (childPageId == nodePageId) {
                    throw new IllegalStateException("内部节点 " + nodePageId + " 自引用");
                }

                return searchHelper(childPageId, key, depth + 1, visitedPages);
            } else {
                throw new IllegalStateException("未知的节点类型: " + nodeType + " 在页面: " + nodePageId);
            }
        } finally {
            visitedPages.remove(nodePageId);
        }
    }

    @Override
    public void delete(String key) {
        if (rootPageId == -1) return;
        Set<Integer> visitedPages = new HashSet<>();
        deleteHelper(rootPageId, key, 0, visitedPages);
    }

    private boolean deleteHelper(int nodePageId, String key, int depth, Set<Integer> visitedPages) {
        if (depth > MAX_RECURSION_DEPTH) {
            throw new RuntimeException("删除时最大递归深度超出：B+树结构可能存在循环，深度: " + depth);
        }

        if (nodePageId < 0) return false;

        if (visitedPages.contains(nodePageId)) {
            throw new RuntimeException("删除时检测到循环引用：页面 " + nodePageId + " 已被访问过");
        }

        visitedPages.add(nodePageId);

        try {
            Page page = bufferPool.fetchPage(nodePageId);
            int nodeType = page.readInt(0);

            if (nodeType == 1) { // 叶子节点
                LeafNode leaf = new LeafNode(nodePageId, maxKeysPerNode);
                leaf.deserialize(page);

                boolean deleted = leaf.removeKey(key);
                if (deleted) {
                    leaf.serialize(page);
                    bufferPool.unpinPage(nodePageId, true);
                    return true;
                } else {
                    bufferPool.unpinPage(nodePageId, false);
                    return false;
                }
            } else if (nodeType == 0) { // 内部节点
                InternalNode internal = new InternalNode(nodePageId, maxKeysPerNode);
                internal.deserialize(page);
                bufferPool.unpinPage(nodePageId, false);

                int childPageId = internal.findChild(key);
                if (childPageId < 0 || childPageId == nodePageId) {
                    throw new IllegalStateException("内部节点 " + nodePageId + " 返回无效的子节点ID: " + childPageId);
                }

                return deleteHelper(childPageId, key, depth + 1, visitedPages);
            } else {
                throw new IllegalStateException("未知的节点类型: " + nodeType + " 在页面: " + nodePageId);
            }
        } finally {
            visitedPages.remove(nodePageId);
        }
    }

    @Override
    public Statistics getStat() {
        if (rootPageId == -1) {
            return new Statistics(0.0, 0, 0, splitCount, mergeCount, bufferPool.getPageAccessCount());
        }

        Set<Integer> visitedPages = new HashSet<>();
        int[] stats = calculateTreeStats(rootPageId, 0, visitedPages);
        int height = stats[0];
        int nodeCount = stats[1];
        int totalSlots = stats[2];
        int usedSlots = stats[3];

        double fillRate = totalSlots > 0 ? (double) usedSlots / totalSlots : 0.0;

        return new Statistics(fillRate, height, nodeCount, splitCount, mergeCount, bufferPool.getPageAccessCount());
    }

    private int[] calculateTreeStats(int nodePageId, int currentHeight, Set<Integer> visitedPages) {
        if (nodePageId < 0 || visitedPages.contains(nodePageId)) {
            return new int[]{0, 0, 0, 0};
        }

        visitedPages.add(nodePageId);

        try {
            Page page = bufferPool.fetchPage(nodePageId);
            int nodeType = page.readInt(0);

            if (nodeType == 1) { // 叶子节点
                LeafNode leaf = new LeafNode(nodePageId, maxKeysPerNode);
                leaf.deserialize(page);
                bufferPool.unpinPage(nodePageId, false);

                return new int[]{currentHeight + 1, 1, maxKeysPerNode, leaf.getKeyCount()};
            } else if (nodeType == 0) { // 内部节点
                InternalNode internal = new InternalNode(nodePageId, maxKeysPerNode);
                internal.deserialize(page);
                bufferPool.unpinPage(nodePageId, false);

                int maxHeight = currentHeight + 1;
                int totalNodes = 1;
                int totalSlots = maxKeysPerNode;
                int usedSlots = internal.getKeyCount();

                for (int i = 0; i <= internal.getKeyCount(); i++) {
                    int childPageId = internal.getChildPageIds()[i];
                    if (childPageId != -1 && !visitedPages.contains(childPageId)) {
                        int[] childStats = calculateTreeStats(childPageId, currentHeight + 1, visitedPages);
                        maxHeight = Math.max(maxHeight, childStats[0]);
                        totalNodes += childStats[1];
                        totalSlots += childStats[2];
                        usedSlots += childStats[3];
                    }
                }

                return new int[]{maxHeight, totalNodes, totalSlots, usedSlots};
            } else {
                return new int[]{0, 0, 0, 0};
            }
        } finally {
            visitedPages.remove(nodePageId);
        }
    }

    // 验证树结构完整性
    private void validateTreeStructure() {
        if (rootPageId == -1) return;

        Set<Integer> visitedPages = new HashSet<>();
        validateNode(rootPageId, -1, visitedPages, 0);
    }

    private void validateNode(int nodePageId, int expectedParent, Set<Integer> visitedPages, int depth) {
        if (depth > MAX_RECURSION_DEPTH) {
            throw new RuntimeException("验证时检测到过深的树结构，可能存在循环");
        }

        if (nodePageId < 0 || visitedPages.contains(nodePageId)) {
            return;
        }

        visitedPages.add(nodePageId);

        try {
            Page page = bufferPool.fetchPage(nodePageId);
            int nodeType = page.readInt(0);

            if (nodeType == 0) { // 内部节点
                InternalNode internal = new InternalNode(nodePageId, maxKeysPerNode);
                internal.deserialize(page);
                bufferPool.unpinPage(nodePageId, false);

                if (internal.getParentPageId() != expectedParent) {
                    System.err.println("警告：节点 " + nodePageId + " 的父节点指针不正确，期望: " + expectedParent + ", 实际: " + internal.getParentPageId());
                }

                // 检查是否存在自引用
                for (int i = 0; i <= internal.getKeyCount(); i++) {
                    int childPageId = internal.getChildPageIds()[i];
                    if (childPageId == nodePageId) {
                        throw new RuntimeException("检测到自引用：内部节点 " + nodePageId + " 指向自己");
                    }
                }

                // 递归验证子节点
                for (int i = 0; i <= internal.getKeyCount(); i++) {
                    int childPageId = internal.getChildPageIds()[i];
                    if (childPageId != -1) {
                        validateNode(childPageId, nodePageId, visitedPages, depth + 1);
                    }
                }
            } else if (nodeType == 1) { // 叶子节点
                LeafNode leaf = new LeafNode(nodePageId, maxKeysPerNode);
                leaf.deserialize(page);
                bufferPool.unpinPage(nodePageId, false);

                if (leaf.getParentPageId() != expectedParent) {
                    System.err.println("警告：叶子节点 " + nodePageId + " 的父节点指针不正确，期望: " + expectedParent + ", 实际: " + leaf.getParentPageId());
                }
            }
        } finally {
            visitedPages.remove(nodePageId);
        }
    }

    private int calculateMaxKeys(int pageSize) {
        // 更准确的计算叶子节点的最大key数量
        // 页面头部：节点类型(4) + keyCount(4) + parentPageId(4) + nextLeafPageId(4) = 16字节

        int headerSize = 16;
        int availableSpace = pageSize - headerSize;

        // 每个entry的实际大小：
        // - key字段: 长度(4) + 内容(最多60) = 64字节
        // - rowId字段: 长度(4) + 内容(最多60) = 64字节
        // - valueCount字段: 4字节
        // - values数组: 假设平均3个value，每个64字节 = 192字节
        // 总计: 64 + 64 + 4 + 192 = 324字节每个entry

        int entrySize = 324;
        int maxKeys = availableSpace / entrySize;

        // 保守一点，减少10%避免边界问题
        maxKeys = (int) (maxKeys * 0.9);

        // 确保至少有3个key，但不超过合理限制
        maxKeys = Math.max(3, Math.min(maxKeys, 20));

        System.out.println("计算得到的maxKeys: " + maxKeys + " (pageSize: " + pageSize + ", availableSpace: " + availableSpace + ", entrySize: " + entrySize + ")");
        return maxKeys;
    }

    private int allocateNewPage() {
        return nextPageId++;
    }

    private void saveMetadata() {
        try {
            byte[] metadata = new byte[16384];
            ByteBuffer buffer = ByteBuffer.wrap(metadata);
            buffer.putInt(0, 0x12345678); // magic number
            buffer.putInt(4, rootPageId);
            buffer.putInt(8, nextPageId);
            buffer.putInt(12, splitCount);
            buffer.putInt(16, mergeCount);
            diskManager.writeMetadata(metadata);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save metadata", e);
        }
    }

    private void updateNodeParent(BPlusTreeNode node) {
        Page page = bufferPool.fetchPage(node.getPageId());
        if (node.isLeaf()) {
            LeafNode leaf = new LeafNode(node.getPageId(), maxKeysPerNode);
            leaf.deserialize(page);
            leaf.setParentPageId(node.getParentPageId());
            leaf.serialize(page);
        } else {
            InternalNode internal = new InternalNode(node.getPageId(), maxKeysPerNode);
            internal.deserialize(page);
            internal.setParentPageId(node.getParentPageId());
            internal.serialize(page);
        }
        bufferPool.unpinPage(node.getPageId(), true);
    }

    private void updateChildrenParent(InternalNode parent) {
        for (int i = 0; i <= parent.getKeyCount(); i++) {
            int childPageId = parent.getChildPageIds()[i];
            if (childPageId != -1) {
                Page childPage = bufferPool.fetchPage(childPageId);
                int nodeType = childPage.readInt(0);

                if (nodeType == 0) { // 内部节点
                    InternalNode child = new InternalNode(childPageId, maxKeysPerNode);
                    child.deserialize(childPage);
                    child.setParentPageId(parent.getPageId());
                    child.serialize(childPage);
                } else if (nodeType == 1) { // 叶子节点
                    LeafNode child = new LeafNode(childPageId, maxKeysPerNode);
                    child.deserialize(childPage);
                    child.setParentPageId(parent.getPageId());
                    child.serialize(childPage);
                }

                bufferPool.unpinPage(childPageId, true);
            }
        }
    }
}