package cn.itcast.hadoop.mapreduce.text;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @ClassName JavaMapDemo
 * @Description
 * @Created by MengYao
 * @Date 2020/10/26 11:49
 * @Version V1.0
 */
public class JavaMapDemo {

    public static void main(String[] args) {
        /**
         * 创建TreeMap
         *      字典序升序（默认）：new TreeMap<>((o1, o2) -> o1.compareTo(o2));
         *      字典序降序：new TreeMap<>((o1, o2) -> o2.compareTo(o1));
         */
        TreeMap<String, String> treeMap1 = new TreeMap<>((o1, o2) -> o2.compareTo(o1));
        // 插入数据
        treeMap1.put("f", "France");// 3 法国
        treeMap1.put("s","Spain");// 6 西班牙
        treeMap1.put("r", "Russia");// 5 俄罗斯
        treeMap1.put("a", "Austria");// 1 奥地利
        treeMap1.put("c", "China");// 2 中国
        treeMap1.put("g", "Germany");// 4 德国
        // 字典序输出
        treeMap1.forEach((k,v)->System.out.println(k+" = "+v));
        // 创建SortedMap
        SortedMap<Long, String> sortedMap1 = new TreeMap<>();
        sortedMap1.put(3L, "France");// 3 法国
        sortedMap1.put(6L,"Spain");// 6 西班牙
        sortedMap1.put(5L, "Russia");// 5 俄罗斯
        sortedMap1.put(1L, "Austria");// 1 奥地利
        sortedMap1.put(2L, "China");// 2 中国
        sortedMap1.put(4L, "Germany");// 4 德国
        // 升序输出
        sortedMap1.forEach((k,v)->System.out.println(k+" = "+v));
    }

}
