package cn.weeg.exp.databaseDesign.impl2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author weeGiam
 */

public class Page {
    public static final int PAGE_SIZE = 4096;
    private final int pageId;
    private final ByteBuffer data;
    private boolean dirty;
    private int pinCount;

    public Page(int pageId) {
        this.pageId = pageId;
        this.data = ByteBuffer.allocate(PAGE_SIZE);
        this.dirty = false;
        this.pinCount = 0;
    }

    public Page(int pageId, byte[] pageData) {
        this.pageId = pageId;
        this.data = ByteBuffer.wrap(pageData);
        this.dirty = false;
        this.pinCount = 0;
    }

    public int getPageId() { return pageId; }
    public ByteBuffer getData() { return data; }
    public boolean isDirty() { return dirty; }
    public void setDirty(boolean dirty) { this.dirty = dirty; }
    public int getPinCount() { return pinCount; }
    public void pin() { pinCount++; }
    public void unpin() { pinCount--; }

    // 读写字符串方法 - 添加边界检查
    public void writeString(int offset, String str, int maxLength) {
        if (offset < 0 || offset + maxLength > PAGE_SIZE) {
            throw new IndexOutOfBoundsException(
                    String.format("写入字符串越界: offset=%d, maxLength=%d, pageSize=%d",
                            offset, maxLength, PAGE_SIZE));
        }

        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(bytes.length, maxLength - 4);

        // 检查是否有足够空间写入长度字段
        if (offset + 4 > PAGE_SIZE) {
            throw new IndexOutOfBoundsException("写入长度字段越界: offset=" + offset);
        }

        data.putInt(offset, length);

        // 检查是否有足够空间写入字符串内容
        if (offset + 4 + length > PAGE_SIZE) {
            throw new IndexOutOfBoundsException(
                    String.format("写入字符串内容越界: offset=%d, length=%d, pageSize=%d",
                            offset + 4, length, PAGE_SIZE));
        }

        data.position(offset + 4);
        data.put(bytes, 0, length);
        setDirty(true);
    }

    public String readString(int offset, int maxLength) {
        if (offset < 0 || offset + 4 > PAGE_SIZE) {
            return "";
        }

        int length = data.getInt(offset);
        if (length <= 0 || length > maxLength - 4 || offset + 4 + length > PAGE_SIZE) {
            return "";
        }

        byte[] bytes = new byte[length];
        data.position(offset + 4);
        data.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public void writeInt(int offset, int value) {
        if (offset < 0 || offset + 4 > PAGE_SIZE) {
            throw new IndexOutOfBoundsException("写入整数越界: offset=" + offset);
        }
        data.putInt(offset, value);
        setDirty(true);
    }

    public int readInt(int offset) {
        if (offset < 0 || offset + 4 > PAGE_SIZE) {
            return 0;
        }
        return data.getInt(offset);
    }

    public void clear() {
        data.clear();
        for (int i = 0; i < PAGE_SIZE; i++) {
            data.put((byte) 0);
        }
        data.rewind();
        setDirty(true);
    }

    // 获取剩余可用空间
    public int getRemainingSpace(int currentOffset) {
        return Math.max(0, PAGE_SIZE - currentOffset);
    }
}