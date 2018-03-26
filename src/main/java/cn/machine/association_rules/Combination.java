package cn.machine.association_rules;

import java.util.*;

/**
 * Created by zhao on 2018-03-02.
 */
public class Combination {

    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {
        List<List<T>> result = new ArrayList<List<T>>();
        for (int i = 0; i <= elements.size(); i++) {
            result.addAll(findSortedCombinations(elements,i));
        }
        return result;
    }

    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<List<T>>();
        if (n == 0) {
            result.add(new ArrayList<T>());
            return result;
        }

        List<List<T>> combinations = findSortedCombinations(elements, n-1);
        for (List<T> combination : combinations) {
            for (T element : elements) {
                if (combination.contains(element)) {
                    continue;
                }

                List<T> list = new ArrayList<T>();
                list.addAll(combination);
                list.add(element);

                Collections.sort(list);
                if (result.contains(list)) {
                    continue;
                }
                result.add(list);
            }
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        test();
    }

    public static void test() throws Exception{
        List<String> list = Arrays.asList("a", "b", "c", "d");
        System.out.println("list="+list);
        List<List<String>> comb = findSortedCombinations(list);
        System.out.println(comb.size());
        System.out.println(comb);
    }

}
