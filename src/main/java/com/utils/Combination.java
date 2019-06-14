package com.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Combination {


    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {

        List<List<T>> result = new ArrayList<>();
        for (int i = 0; i <= elements.size(); i++) {
            result.addAll(findSortedCombinations(elements, i));
        }

        return result;

    }


    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<>();
        if (n == 0) {
            result.add(new ArrayList<>());
            return result;
        }

        List<List<T>> combinations = findSortedCombinations(elements, n - 1);
        System.out.println(combinations);
        for (List<T> combination : combinations) {
            for (T element : elements) {
                if (combination.contains(element)) {
                    continue;
                }

                List<T> list = new ArrayList<>();
                list.addAll(combination);
                if (list.contains(element)) {
                    continue;
                }

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

    /**
     * Basic Test of findSortedCombinations()
     *
     * @param args
     */
    public static void main(String[] args) {
        List<String> elements = Arrays.asList("a", "b", "c", "d", "e");
        List<List<String>> combinations = findSortedCombinations(elements, 3);
        System.out.println(combinations);
    }
}
