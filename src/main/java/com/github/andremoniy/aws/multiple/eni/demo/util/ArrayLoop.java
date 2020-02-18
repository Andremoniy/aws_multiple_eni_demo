package com.github.andremoniy.aws.multiple.eni.demo.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ArrayLoop<E> implements Loop<E> {

    private final List<E> elements;
    private final AtomicReference<Iterator<E>> iteratorRef;

    public ArrayLoop(final List<E> elements) {
        this.elements = Collections.unmodifiableList(elements);
        this.iteratorRef = new AtomicReference<>();
    }

    @Override
    public E getNext() {
        if (iteratorRef.get() == null || !iteratorRef.get().hasNext()) {
            iteratorRef.set(elements.iterator());
        }
        return iteratorRef.get().next();
    }
}
