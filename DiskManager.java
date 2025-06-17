package cn.weeg.exp.databaseDesign.impl2;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author weeGiam
 */

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DiskManager {
    private RandomAccessFile file;
    private String filename;

    public void openFile(String filename, int pageSize) throws IOException {
        this.filename = filename;
        boolean exists = Files.exists(Paths.get(filename));
        this.file = new RandomAccessFile(filename, "rw");

        if (!exists) {
            // 创建元数据页面（16KB）
            byte[] metadata = new byte[16384];
            file.write(metadata);
            file.getFD().sync();
        }
    }

    public void closeFile() throws IOException {
        if (file != null) {
            file.close();
        }
    }

    public Page readPage(int pageId, int pageSize) throws IOException {
        if (file == null || pageId < 0) {
            // 对于无效的页面ID，返回新的空页面
            return new Page(pageId);
        }

        long offset = 16384L + (long) pageId * pageSize; // 跳过16KB元数据
        byte[] data = new byte[pageSize];

        // 检查文件是否足够大
        long fileLength = file.length();
        if (offset >= fileLength) {
            // 文件不够大，返回全零的页面
            return new Page(pageId, data); // data已经是全零数组
        }

        file.seek(offset);
        int bytesRead = file.read(data);

        if (bytesRead == -1) {
            // 到达文件末尾，没有读取到任何数据，使用全零数组
            data = new byte[pageSize]; // 重新创建全零数组
        } else if (bytesRead < pageSize) {
            // 部分读取，填充剩余部分为零
            for (int i = bytesRead; i < pageSize; i++) {
                data[i] = 0;
            }
        }

        return new Page(pageId, data);
    }

    public void writePage(Page page, int pageSize) throws IOException {
        if (file == null || page.getPageId() < 0) return;

        long offset = 16384L + (long) page.getPageId() * pageSize;

        // 确保文件足够大
        long fileLength = file.length();
        if (offset + pageSize > fileLength) {
            // 扩展文件大小
            file.setLength(offset + pageSize);
        }

        file.seek(offset);

        byte[] data = new byte[pageSize];
        page.getData().rewind();
        page.getData().get(data);

        file.write(data);
        file.getFD().sync();
        page.setDirty(false);
    }

    public void writeMetadata(byte[] metadata) throws IOException {
        if (file == null) return;
        file.seek(0);
        file.write(metadata);
        file.getFD().sync();
    }

    public byte[] readMetadata() throws IOException {
        if (file == null) return new byte[16384];

        // 检查文件是否足够大来读取元数据
        if (file.length() < 16384) {
            // 文件太小，返回全零的元数据
            return new byte[16384];
        }

        file.seek(0);
        byte[] metadata = new byte[16384];
        int bytesRead = file.read(metadata);

        if (bytesRead == -1) {
            // 到达文件末尾，返回全零数组
            return new byte[16384];
        } else if (bytesRead < 16384) {
            // 部分读取，填充剩余部分为零
            for (int i = bytesRead; i < 16384; i++) {
                metadata[i] = 0;
            }
        }

        return metadata;
    }
}