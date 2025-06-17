package cn.weeg.exp.databaseDesign.impl2;

/**
 * @author weeGiam
 */


public abstract class BPlusTreeNode {
    protected int pageId;
    protected boolean isLeaf;
    protected int keyCount;
    protected String[] keys;
    protected int parentPageId;

    public BPlusTreeNode(int pageId, boolean isLeaf, int maxKeys) {
        this.pageId = pageId;
        this.isLeaf = isLeaf;
        this.keyCount = 0;
        this.keys = new String[maxKeys];
        this.parentPageId = -1;
    }

    public abstract void serialize(Page page);

    public abstract void deserialize(Page page);

    public abstract boolean isFull();

    public abstract boolean isUnderflow();

    // Getters and setters
    public int getPageId() {
        return pageId;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public String[] getKeys() {
        return keys;
    }

    public int getParentPageId() {
        return parentPageId;
    }

    public void setParentPageId(int parentPageId) {
        this.parentPageId = parentPageId;
    }

    protected int binarySearch(String key) {
        int left = 0, right = keyCount - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            int cmp = keys[mid].compareTo(key);
            if (cmp == 0) return mid;
            else if (cmp < 0) left = mid + 1;
            else right = mid - 1;
        }
        return left;
    }
}
