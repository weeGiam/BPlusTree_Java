package cn.weeg.exp.databaseDesign.impl2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author weeGiam
 */

public class BPlusTreeTest {

    public static void main(String[] args) {
        System.out.println("开始B+树系统测试...\n");

        // 基本功能测试
        testBasicOperations();

        // 大数据量性能测试
        testPerformanceWith5Million();

        System.out.println("所有测试完成！");
    }

    private static void testBasicOperations() {
        System.out.println("=== 基本功能测试 ===");

        BPlusTree tree = new BPlusTreeImpl();
        String filename = "test_basic.db";

        try {
            // 创建B+树
            tree.create(filename, 4096);
            System.out.println("✓ 1.B+树创建成功");

            // 插入测试数据
            System.out.println("插入测试数据...");
            for (int i = 0; i < 100; i++) {
                System.out.println("2." + (i+1) + " 插入第" + (i+1) + "条记录...");
                String key = String.format("key_%04d", i);
                String[] values = {
                        "value_" + i + "_1",
                        "value_" + i + "_2",
                        "value_" + i + "_3"
                };
                String rowId = "row_" + i;
                tree.insert(key, values, rowId);
                System.out.println("✓ 第" + (i+1) + "条记录插入成功: " + key);

            }
            System.out.println("✓ 插入100条记录");

            // 查询测试
            System.out.println("查询测试...");
            String[][] results = tree.get("key_0050");
            if (results.length > 0 && results[0].length == 3) {
                System.out.println("✓ 查询成功: " + String.join(", ", results[0]));
            } else {
                System.out.println("✗ 查询失败");
            }

            // 删除测试
            tree.delete("key_0050");
            results = tree.get("key_0050");
            if (results.length == 0) {
                System.out.println("✓ 删除成功");
            } else {
                System.out.println("✗ 删除失败");
            }

            // 统计信息
            Statistics stats = tree.getStat();
            System.out.println("✓ 统计信息: " + stats);

            tree.close(filename);
            System.out.println("✓ B+树关闭成功\n");

        } catch (Exception e) {
            System.out.println("✗ 基本功能测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testPerformanceWith5Million() {
        System.out.println("=== 500万数据性能测试 ===");

        BPlusTree tree = new BPlusTreeImpl();
        String filename = "test_5million.db";
        Random random = new Random(42); // 固定种子保证可重复性

        try {
            tree.create(filename, 4096);

            System.out.println("开始插入500万条随机数据...");
            long startTime = System.currentTimeMillis();

            // 生成500万条随机数据
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < 5_000_000; i++) {
                String key = String.format("key_%08d", random.nextInt(10_000_000));
                keys.add(key);

                String[] values = {
                        "value_" + i + "_field1",
                        "value_" + i + "_field2",
                        "value_" + i + "_field3",
                        "value_" + i + "_field4"
                };
                String rowId = "row_" + i;

                tree.insert(key, values, rowId);

                if ((i + 1) % 500_000 == 0) {
                    long currentTime = System.currentTimeMillis();
                    double avgTime = (currentTime - startTime) / (double) (i + 1);
                    System.out.printf("已插入 %,d 条记录，平均 %.3f ms/条\n", i + 1, avgTime);
                }
            }

            long insertTime = System.currentTimeMillis() - startTime;
            System.out.printf("✓ 插入完成，总耗时: %.2f 秒\n", insertTime / 1000.0);

            // 获取插入后的统计信息
            Statistics insertStats = tree.getStat();
            System.out.println("插入后统计: " + insertStats);

            // 查询性能测试
            System.out.println("\n开始查询性能测试...");
            startTime = System.currentTimeMillis();
            int queryCount = 10000;
            int foundCount = 0;

            for (int i = 0; i < queryCount; i++) {
                String queryKey = keys.get(random.nextInt(keys.size()));
                String[][] results = tree.get(queryKey);
                if (results.length > 0) {
                    foundCount++;
                }
            }

            long queryTime = System.currentTimeMillis() - startTime;
            Statistics queryStats = tree.getStat();

            System.out.printf("✓ 查询完成: %d/%d 条记录找到\n", foundCount, queryCount);
            System.out.printf("✓ 查询总耗时: %.2f 秒\n", queryTime / 1000.0);
            System.out.printf("✓ 平均查询时间: %.3f ms\n", queryTime / (double) queryCount);
            System.out.printf("✓ 平均页面访问量: %.2f 页/查询\n",
                    queryStats.getPageAccessCount() / (double) queryCount);

            // 最终统计信息
            System.out.println("\n最终统计信息:");
            System.out.println("- 填充率: " + String.format("%.2f%%", queryStats.getFillRate() * 100));
            System.out.println("- 树高度: " + queryStats.getHeight());
            System.out.println("- 节点数量: " + queryStats.getNodeCount());
            System.out.println("- 分裂次数: " + queryStats.getSplitCount());
            System.out.println("- 合并次数: " + queryStats.getMergeCount());
            System.out.println("- 总页面访问: " + queryStats.getPageAccessCount());

            tree.close(filename);
            System.out.println("✓ 性能测试完成\n");

        } catch (Exception e) {
            System.out.println("✗ 性能测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 辅助方法：测试数据一致性
    private static void testDataConsistency() {
        System.out.println("=== 数据一致性测试 ===");

        BPlusTree tree = new BPlusTreeImpl();
        String filename = "test_consistency.db";

        try {
            tree.create(filename, 4096);

            // 插入有序数据
            for (int i = 0; i < 1000; i++) {
                String key = String.format("key_%04d", i);
                String[] values = {"value_" + i};
                tree.insert(key, values, "row_" + i);
            }

            // 验证查询结果
            boolean allFound = true;
            for (int i = 0; i < 1000; i++) {
                String key = String.format("key_%04d", i);
                String[][] results = tree.get(key);
                if (results.length == 0 || !results[0][0].equals("value_" + i)) {
                    allFound = false;
                    break;
                }
            }

            if (allFound) {
                System.out.println("✓ 数据一致性测试通过");
            } else {
                System.out.println("✗ 数据一致性测试失败");
            }

            tree.close(filename);

        } catch (Exception e) {
            System.out.println("✗ 数据一致性测试异常: " + e.getMessage());
        }
    }
}
