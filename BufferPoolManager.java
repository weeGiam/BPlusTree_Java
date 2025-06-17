package cn.weeg.exp.databaseDesign.impl2;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author weeGiam
 */

public class BufferPoolManager {
    private final int poolSize;
    private final int pageSize;
    private final Map<Integer, Page> pageTable;
    private final LinkedHashMap<Integer, Page> lruList;
    private final DiskManager diskManager;
    private int pageAccessCount = 0;

    public BufferPoolManager(int poolSize, int pageSize, DiskManager diskManager) {
        this.poolSize = poolSize;
        this.pageSize = pageSize;
        this.diskManager = diskManager;
        this.pageTable = new HashMap<>();
        this.lruList = new LinkedHashMap<Integer, Page>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Page> eldest) {
                return size() > poolSize;
            }
        };
    }

    public Page fetchPage(int pageId) {
        if (pageId < 0) {
            throw new IllegalArgumentException("Invalid page ID: " + pageId);
        }

        pageAccessCount++;

        Page page = pageTable.get(pageId);
        if (page != null) {
            page.pin();
            lruList.put(pageId, page); // 更新LRU顺序
            return page;
        }

        // 需要从磁盘读取
        try {
            page = diskManager.readPage(pageId, pageSize);
            if (page == null) {
                page = new Page(pageId);
            }

            // 检查是否需要驱逐页面
            if (pageTable.size() >= poolSize) {
                evictPage();
            }

            page.pin();
            pageTable.put(pageId, page);
            lruList.put(pageId, page);
            return page;

        } catch (IOException e) {
            throw new RuntimeException("Failed to read page " + pageId, e);
        }
    }

    public void unpinPage(int pageId, boolean isDirty) {
        Page page = pageTable.get(pageId);
        if (page != null) {
            page.unpin();
            if (isDirty) {
                page.setDirty(true);
            }
        }
    }

    public boolean flushPage(int pageId) {
        Page page = pageTable.get(pageId);
        if (page != null && page.isDirty()) {
            try {
                diskManager.writePage(page, pageSize);
                return true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush page " + pageId, e);
            }
        }
        return false;
    }

    public void flushAllPages() {
        for (Page page : pageTable.values()) {
            if (page.isDirty()) {
                try {
                    diskManager.writePage(page, pageSize);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to flush page " + page.getPageId(), e);
                }
            }
        }
    }

    private void evictPage() {
        // 找到第一个未被pin的页面进行驱逐
        for (Map.Entry<Integer, Page> entry : lruList.entrySet()) {
            Page page = entry.getValue();
            if (page.getPinCount() == 0) {
                if (page.isDirty()) {
                    flushPage(page.getPageId());
                }
                pageTable.remove(entry.getKey());
                lruList.remove(entry.getKey());
                return;
            }
        }
        throw new RuntimeException("No page can be evicted");
    }

    public int getPageAccessCount() {
        return pageAccessCount;
    }

    public void resetPageAccessCount() {
        pageAccessCount = 0;
    }
}