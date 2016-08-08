package net.github.gearman.server.common;

import java.util.Map;

public interface Series<I,V> {
    Index<I> index();
    Map<I, V> data();
    void expand(Index<I> newIndex);
    V lastValue();
    Map<I, V> dataWindow(I lower, I upper);
}
