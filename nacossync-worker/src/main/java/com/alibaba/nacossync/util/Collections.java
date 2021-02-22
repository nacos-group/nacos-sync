package com.alibaba.nacossync.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public abstract class Collections {

    public static Collection subtract(final Collection a, final Collection b) {
        ArrayList list = new ArrayList( a );
        for (Iterator it = b.iterator(); it.hasNext();) {
            list.remove(it.next());
        }
        return list;
    }



}
