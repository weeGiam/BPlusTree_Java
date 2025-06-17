package cn.weeg.exp.databaseDesign.impl2;

/**
 * @author weeGiam
 */

public class LeafNode extends BPlusTreeNode {
    private String[][] values; // 每个key对应的value数组
    private String[] rowIds;
    private int nextLeafPageId;
    private final int maxKeys;

    public LeafNode(int pageId, int maxKeys) {
        super(pageId, true, maxKeys);
        this.maxKeys = maxKeys;
        this.values = new String[maxKeys][];
        this.rowIds = new String[maxKeys];
        this.nextLeafPageId = -1;
    }

    @Override
    public void serialize(Page page) {
        page.clear();
        int offset = 0;

        try {
            // 写入节点类型
            page.writeInt(offset, 1); // 1表示叶子节点
            offset += 4;

            // 写入key数量
            page.writeInt(offset, keyCount);
            offset += 4;

            // 写入父节点页面ID
            page.writeInt(offset, parentPageId);
            offset += 4;

            // 写入下一个叶子节点页面ID
            page.writeInt(offset, nextLeafPageId);
            offset += 4;

            System.out.println("开始序列化叶子节点，keyCount=" + keyCount + ", 起始offset=" + offset);

            // 写入keys, values, rowIds
            for (int i = 0; i < keyCount; i++) {
                // 检查剩余空间
                int remainingSpace = page.getRemainingSpace(offset);
                if (remainingSpace < 200) { // 保守估计每个entry至少需要200字节
                    throw new RuntimeException(
                            String.format("页面空间不足: entry %d, remainingSpace=%d, offset=%d",
                                    i, remainingSpace, offset));
                }

                // 写入key
                page.writeString(offset, keys[i] != null ? keys[i] : "", 64);
                offset += 64;

                // 写入rowId
                page.writeString(offset, rowIds[i] != null ? rowIds[i] : "", 64);
                offset += 64;

                // 写入values数组
                if (values[i] != null) {
                    page.writeInt(offset, values[i].length);
                    offset += 4;

                    for (String value : values[i]) {
                        // 再次检查空间
                        if (page.getRemainingSpace(offset) < 64) {
                            throw new RuntimeException("序列化value时空间不足");
                        }
                        page.writeString(offset, value != null ? value : "", 64);
                        offset += 64;
                    }
                } else {
                    page.writeInt(offset, 0);
                    offset += 4;
                }

                System.out.println("  序列化entry " + i + ", key=" + keys[i] + ", offset=" + offset);
            }

            System.out.println("叶子节点序列化完成，最终offset=" + offset);

        } catch (Exception e) {
            System.err.println("序列化失败在offset=" + offset + ", keyCount=" + keyCount);
            throw new RuntimeException("叶子节点序列化失败", e);
        }
    }

    @Override
    public void deserialize(Page page) {
        int offset = 4; // 跳过节点类型

        keyCount = page.readInt(offset);
        offset += 4;

        parentPageId = page.readInt(offset);
        offset += 4;

        nextLeafPageId = page.readInt(offset);
        offset += 4;

        for (int i = 0; i < keyCount && i < maxKeys; i++) {
            keys[i] = page.readString(offset, 64);
            offset += 64;

            rowIds[i] = page.readString(offset, 64);
            offset += 64;

            int valueCount = page.readInt(offset);
            offset += 4;

            if (valueCount > 0) {
                values[i] = new String[valueCount];
                for (int j = 0; j < valueCount; j++) {
                    values[i][j] = page.readString(offset, 64);
                    offset += 64;
                }
            }
        }
    }

    @Override
    public boolean isFull() {
        return keyCount >= maxKeys;
    }

    @Override
    public boolean isUnderflow() {
        return keyCount < maxKeys / 2;
    }

    public void insertKeyValue(String key, String[] value, String rowId) {
        // 检查是否已满 - 修改为更宽松的检查
        if (keyCount >= maxKeys) {
            throw new IllegalStateException("叶子节点已满，无法插入更多键值对。keyCount=" + keyCount + ", maxKeys=" + maxKeys);
        }

        int pos = binarySearch(key);

        // 如果key已存在，更新value
        if (pos < keyCount && keys[pos] != null && keys[pos].equals(key)) {
            values[pos] = value;
            rowIds[pos] = rowId;
            System.out.println("更新现有key: " + key + " at position " + pos);
            return;
        }

        // 确保pos在有效范围内
        if (pos < 0) pos = 0;
        if (pos > keyCount) pos = keyCount;

        // 检查插入位置是否会导致数组越界
        if (pos >= maxKeys) {
            throw new IllegalStateException("插入位置超出数组边界: pos=" + pos + ", maxKeys=" + maxKeys);
        }

        // 向右移动现有元素为新元素腾出空间
        for (int i = keyCount; i > pos; i--) {
            if (i >= maxKeys) continue; // 跳过超出边界的索引
            keys[i] = keys[i - 1];
            values[i] = values[i - 1];
            rowIds[i] = rowIds[i - 1];
        }

        // 插入新的key-value
        keys[pos] = key;
        values[pos] = value;
        rowIds[pos] = rowId;
        keyCount++;

        // 调试输出
        System.out.println("插入到叶子节点: key=" + key + ", pos=" + pos + ", keyCount=" + keyCount + ", maxKeys=" + maxKeys);
    }
    public String[][] search(String key) {
        int pos = binarySearch(key);
        if (pos < keyCount && keys[pos] != null && keys[pos].equals(key)) {
            return new String[][]{values[pos]};
        }
        return new String[0][];
    }

    public boolean removeKey(String key) {
        int pos = binarySearch(key);
        if (pos < keyCount && keys[pos] != null && keys[pos].equals(key)) {
            // 向左移动后面的元素
            for (int i = pos; i < keyCount - 1; i++) {
                keys[i] = keys[i + 1];
                values[i] = values[i + 1];
                rowIds[i] = rowIds[i + 1];
            }

            // 清空最后一个位置
            if (keyCount > 0) {
                keys[keyCount - 1] = null;
                values[keyCount - 1] = null;
                rowIds[keyCount - 1] = null;
            }

            keyCount--;
            return true;
        }
        return false;
    }

    public LeafNode split() {
        if (keyCount < 2) {
            throw new IllegalStateException("叶子节点keys太少，无法分裂");
        }

        int midIndex = keyCount / 2;
        LeafNode newLeaf = new LeafNode(-1, maxKeys); // 页面ID稍后分配

        System.out.println("开始分裂叶子节点: keyCount=" + keyCount + ", midIndex=" + midIndex);

        // 移动后半部分的数据到新节点
        for (int i = midIndex; i < keyCount; i++) {
            int newIndex = i - midIndex;
            if (newIndex < maxKeys) {
                newLeaf.keys[newIndex] = keys[i];
                newLeaf.values[newIndex] = values[i];
                newLeaf.rowIds[newIndex] = rowIds[i];
                newLeaf.keyCount++;

                System.out.println("移动entry到新节点: " + keys[i] + " -> 新节点位置" + newIndex);

                // 清空原节点中已移动的数据
                keys[i] = null;
                values[i] = null;
                rowIds[i] = null;
            }
        }

        // 更新当前节点的key数量
        keyCount = midIndex;

        System.out.println("分裂完成: 原节点keyCount=" + keyCount + ", 新节点keyCount=" + newLeaf.keyCount);
        System.out.println("新节点第一个key: " + (newLeaf.keyCount > 0 ? newLeaf.keys[0] : "null"));

        return newLeaf;
    }

    @Override
    protected int binarySearch(String key) {
        if (key == null) return 0;

        int left = 0, right = keyCount - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            if (keys[mid] == null) {
                right = mid - 1;
                continue;
            }

            int cmp = keys[mid].compareTo(key);
            if (cmp == 0) return mid;
            else if (cmp < 0) left = mid + 1;
            else right = mid - 1;
        }
        return left;
    }

    // Getters and setters
    public String[][] getValues() { return values; }
    public String[] getRowIds() { return rowIds; }
    public int getNextLeafPageId() { return nextLeafPageId; }
    public void setNextLeafPageId(int nextLeafPageId) { this.nextLeafPageId = nextLeafPageId; }
    public void setPageId(int pageId) { this.pageId = pageId; }

    // 调试方法
    public void printNode() {
        System.out.println("LeafNode " + pageId + ":");
        System.out.println("  Parent: " + parentPageId);
        System.out.println("  KeyCount: " + keyCount + "/" + maxKeys);
        System.out.println("  NextLeaf: " + nextLeafPageId);
        System.out.print("  Keys: [");
        for (int i = 0; i < keyCount; i++) {
            System.out.print(keys[i]);
            if (i < keyCount - 1) System.out.print(", ");
        }
        System.out.println("]");
    }
}