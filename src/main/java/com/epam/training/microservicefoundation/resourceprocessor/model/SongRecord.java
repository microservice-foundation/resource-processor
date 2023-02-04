package com.epam.training.microservicefoundation.resourceprocessor.model;

import java.io.Serializable;

public class SongRecord implements Serializable {
    private static final long serialVersionUID = 17_11_2022_22_51L;
    private long id;

    public SongRecord() {
    }

    public SongRecord(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }
}
