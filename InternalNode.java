package cn.weeg.exp.databaseDesign.impl2;

/**
 * @author weeGiam
 */

public class InternalNode extends BPlusTreeNode {
    private int[] childPageIds;
    private final int maxKeys;
    private String splitMiddleKey; // 用于存储分裂时的中间key

    public InternalNode(int pageId, int maxKeys) {
        super(pageId, false, maxKeys);
        this.maxKeys = maxKeys;
        this.childPageIds = new int[maxKeys + 2]; // 增加一个位置用于分裂时的临时存储
        // 初始化所有子节点指针为-1
        for (int i = 0; i < childPageIds.length; i++) {
            childPageIds[i] = -1;
        }
    }

    @Override
    public void serialize(Page page) {
        page.clear();
        int offset = 0;

        // 写入节点类型
        page.writeInt(offset, 0); // 0表示内部节点
        offset += 4;

        // 写入key数量
        page.writeInt(offset, keyCount);
        offset += 4;

        // 写入父节点页面ID
        page.writeInt(offset, parentPageId);
        offset += 4;

        // 写入keys
        for (int i = 0; i < maxKeys; i++) {
            if (i < keyCount && keys[i] != null) {
                page.writeString(offset, keys[i], 64);
            } else {
                page.writeString(offset, "", 64);
            }
            offset += 64;
        }

        // 写入子节点页面IDs
        for (int i = 0; i <= maxKeys; i++) {
            if (i <= keyCount) {
                page.writeInt(offset, childPageIds[i]);
            } else {
                page.writeInt(offset, -1);
            }
            offset += 4;
        }
    }

    @Override
    public void deserialize(Page page) {
        int offset = 4; // 跳过节点类型

        keyCount = page.readInt(offset);
        offset += 4;

        parentPageId = page.readInt(offset);
        offset += 4;

        // 读取keys
        for (int i = 0; i < maxKeys; i++) {
            String key = page.readString(offset, 64);
            if (i < keyCount && !key.isEmpty()) {
                keys[i] = key;
            } else {
                keys[i] = null;
            }
            offset += 64;
        }

        // 读取子节点页面IDs
        for (int i = 0; i <= maxKeys; i++) {
            childPageIds[i] = page.readInt(offset);
            offset += 4;
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

    public int findChild(String key) {
        // 如果没有key，返回第一个子节点
        if (keyCount == 0) {
            return childPageIds[0];
        }

        int pos = 0;
        // 找到第一个大于key的位置
        while (pos < keyCount && keys[pos] != null && key.compareTo(keys[pos]) >= 0) {
            pos++;
        }

        // 确保返回有效的子节点ID
        if (pos <= keyCount && childPageIds[pos] != -1) {
            return childPageIds[pos];
        }

        // 如果找不到有效的子节点，返回第一个有效的子节点
        for (int i = 0; i <= keyCount; i++) {
            if (childPageIds[i] != -1) {
                return childPageIds[i];
            }
        }

        // 如果所有子节点都无效，返回-1
        return -1;
    }

    public void insertKeyChild(String key, int leftChildId, int rightChildId) {
        if (keyCount >= maxKeys) {
            throw new IllegalStateException("内部节点已满，无法插入更多key。keyCount=" + keyCount + ", maxKeys=" + maxKeys);
        }

        int pos = 0;
        // 找到插入位置
        while (pos < keyCount && keys[pos] != null && key.compareTo(keys[pos]) > 0) {
            pos++;
        }

        // 移动现有的keys和子节点指针
        for (int i = keyCount; i > pos; i--) {
            keys[i] = keys[i - 1];
        }
        for (int i = keyCount + 1; i > pos + 1; i--) {
            childPageIds[i] = childPageIds[i - 1];
        }

        // 插入新的key和子节点指针
        keys[pos] = key;
        childPageIds[pos] = leftChildId;
        childPageIds[pos + 1] = rightChildId;
        keyCount++;

        System.out.println("向内部节点插入: key=" + key + ", pos=" + pos + ", keyCount=" + keyCount);
    }

    // 用于分裂时的插入（允许临时超过容量）
    public void insertKeyChildForSplit(String key, int leftChildId, int rightChildId) {
        if (keyCount >= maxKeys + 1) {
            throw new IllegalStateException("内部节点超过分裂容量限制");
        }

        int pos = 0;
        // 找到插入位置
        while (pos < keyCount && keys[pos] != null && key.compareTo(keys[pos]) > 0) {
            pos++;
        }

        // 移动现有的keys和子节点指针
        for (int i = keyCount; i > pos; i--) {
            keys[i] = keys[i - 1];
        }
        for (int i = keyCount + 1; i > pos + 1; i--) {
            childPageIds[i] = childPageIds[i - 1];
        }

        // 插入新的key和子节点指针
        keys[pos] = key;
        childPageIds[pos] = leftChildId;
        childPageIds[pos + 1] = rightChildId;
        keyCount++;

        System.out.println("向内部节点插入(用于分裂): key=" + key + ", pos=" + pos + ", keyCount=" + keyCount);
    }

    public InternalNode split() {
        if (keyCount < 3) {
            throw new IllegalStateException("内部节点keys太少，无法分裂");
        }

        int midIndex = keyCount / 2;
        splitMiddleKey = keys[midIndex]; // 保存中间key，它将被提升到父节点
        InternalNode newInternal = new InternalNode(-1, maxKeys);

        System.out.println("开始分裂内部节点: keyCount=" + keyCount + ", midIndex=" + midIndex + ", midKey=" + splitMiddleKey);

        // 移动后半部分的keys到新节点（不包括中间key）
        for (int i = midIndex + 1; i < keyCount; i++) {
            int newIndex = i - midIndex - 1;
            newInternal.keys[newIndex] = keys[i];
            keys[i] = null; // 清空原位置
            newInternal.keyCount++;
            System.out.println("移动key到新内部节点: " + newInternal.keys[newIndex] + " -> 位置" + newIndex);
        }

        // 移动后半部分的子节点指针到新节点
        for (int i = midIndex + 1; i <= keyCount; i++) {
            int newIndex = i - midIndex - 1;
            newInternal.childPageIds[newIndex] = childPageIds[i];
            childPageIds[i] = -1; // 清空原位置
            System.out.println("移动子节点到新内部节点: " + newInternal.childPageIds[newIndex] + " -> 位置" + newIndex);
        }

        // 清空中间key（它会被提升到父节点）
        keys[midIndex] = null;

        // 更新当前节点的key数量
        keyCount = midIndex;

        System.out.println("内部节点分裂完成: 原节点keyCount=" + keyCount + ", 新节点keyCount=" + newInternal.keyCount);

        return newInternal;
    }

    public String getMiddleKey() {
        if (keyCount == 0) {
            throw new IllegalStateException("内部节点没有keys");
        }
        int midIndex = keyCount / 2;
        return keys[midIndex];
    }

    public String getSplitMiddleKey() {
        return splitMiddleKey;
    }

    // Getters and setters
    public int[] getChildPageIds() { return childPageIds; }
    public void setPageId(int pageId) { this.pageId = pageId; }

    // 调试方法
    public void printNode() {
        System.out.println("InternalNode " + pageId + ":");
        System.out.println("  Parent: " + parentPageId);
        System.out.println("  KeyCount: " + keyCount + "/" + maxKeys);
        System.out.print("  Keys: [");
        for (int i = 0; i < keyCount; i++) {
            System.out.print(keys[i]);
            if (i < keyCount - 1) System.out.print(", ");
        }
        System.out.println("]");
        System.out.print("  Children: [");
        for (int i = 0; i <= keyCount; i++) {
            System.out.print(childPageIds[i]);
            if (i < keyCount) System.out.print(", ");
        }
        System.out.println("]");
    }
}