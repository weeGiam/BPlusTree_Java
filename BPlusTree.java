package cn.weeg.exp.databaseDesign.impl2;

/**
 * @author weeGiam
 */
public interface BPlusTree {
    /**
     * 创建或打开B+树文件
     *
     * @param filename 文件名
     * @param pageSize 页面大小
     */
    void create(String filename, int pageSize);

    /**
     * 关闭B+树文件
     *
     * @param filename 文件名
     */
    void close(String filename);

    /**
     * 插入键值对
     *
     * @param key   键
     * @param value 值数组
     * @param rowId 行ID
     */
    void insert(String key, String[] value, String rowId);

    /**
     * 根据键查询值
     *
     * @param key 键
     * @return 值的二维数组
     */
    String[][] get(String key);

    /**
     * 删除键
     *
     * @param key 键
     */
    void delete(String key);

    /**
     * 获取统计信息
     *
     * @return 统计信息对象
     */
    Statistics getStat();
}