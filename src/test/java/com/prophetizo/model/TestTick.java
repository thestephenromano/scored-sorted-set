package com.prophetizo.model;

public class TestTick {

    private final float price;
    private final int volume;
    private final boolean isAskTick;

    private final long time;

    public TestTick(long time, double price, int volume, boolean isAskTick) {
        this.time = time;
        this.price = (float)price;
        this.volume = volume;
        this.isAskTick = isAskTick;
    }

    public float getPrice() {
        return price;
    }

    public int getVolume() {
        return volume;
    }

    public boolean isAskTick() {
        return isAskTick;
    }

    public long getTime() {
        return time;
    }
}
