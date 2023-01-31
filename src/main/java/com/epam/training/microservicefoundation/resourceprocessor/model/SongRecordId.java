package com.epam.training.microservicefoundation.resourceprocessor.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class SongRecordId implements Serializable {
    private static final long serialVersionUID = 17_11_2022_22_51L;
    @JsonProperty("Id")
    private long id;

    public SongRecordId() {
    }

    public SongRecordId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }
}
